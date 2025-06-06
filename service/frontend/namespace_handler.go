//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination namespace_handler_mock.go

package frontend

import (
	"context"
	"time"

	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsmanager"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// namespaceHandler implements the namespace-related APIs specified by the [workflowservice] package,
	// such as registering, updating, and querying namespaces.
	namespaceHandler struct {
		logger                 log.Logger
		metadataMgr            persistence.MetadataManager
		clusterMetadata        cluster.Metadata
		namespaceReplicator    nsreplication.Replicator
		namespaceAttrValidator *nsmanager.Validator
		archivalMetadata       archiver.ArchivalMetadata
		archiverProvider       provider.ArchiverProvider
		timeSource             clock.TimeSource
		config                 *Config
	}
)

const (
	maxReplicationHistorySize = 10
)

var (
	// err indicating that this cluster is not the master, so cannot do namespace registration or update
	errNotMasterCluster                   = serviceerror.NewInvalidArgument("Cluster is not master cluster, cannot do namespace registration or namespace update.")
	errCannotDoNamespaceFailoverAndUpdate = serviceerror.NewInvalidArgument("Cannot set active cluster to current cluster when other parameters are set.")
	errInvalidRetentionPeriod             = serviceerror.NewInvalidArgument("A valid retention period is not set on request.")
	errInvalidNamespaceStateUpdate        = serviceerror.NewInvalidArgument("Invalid namespace state update.")

	errCustomSearchAttributeFieldAlreadyAllocated = serviceerror.NewInvalidArgument("Custom search attribute field name already allocated.")
)

// newNamespaceHandler create a new namespace handler
func newNamespaceHandler(
	logger log.Logger,
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	namespaceReplicator nsreplication.Replicator,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
	timeSource clock.TimeSource,
	config *Config,
) *namespaceHandler {
	return &namespaceHandler{
		logger:                 logger,
		metadataMgr:            metadataMgr,
		clusterMetadata:        clusterMetadata,
		namespaceReplicator:    namespaceReplicator,
		namespaceAttrValidator: nsmanager.NewValidator(clusterMetadata),
		archivalMetadata:       archivalMetadata,
		archiverProvider:       archiverProvider,
		timeSource:             timeSource,
		config:                 config,
	}
}

// RegisterNamespace register a new namespace
//
//nolint:revive // cognitive complexity grandfathered
func (d *namespaceHandler) RegisterNamespace(
	ctx context.Context,
	registerRequest *workflowservice.RegisterNamespaceRequest,
) (*workflowservice.RegisterNamespaceResponse, error) {

	if !d.clusterMetadata.IsGlobalNamespaceEnabled() {
		if registerRequest.GetIsGlobalNamespace() {
			return nil, serviceerror.NewInvalidArgument("Cannot register global namespace when not enabled")
		}

		registerRequest.IsGlobalNamespace = false
	} else {
		// cluster global namespace enabled
		if !d.clusterMetadata.IsMasterCluster() && registerRequest.GetIsGlobalNamespace() {
			return nil, errNotMasterCluster
		}
	}

	if err := validateRetentionDuration(
		registerRequest.WorkflowExecutionRetentionPeriod,
		registerRequest.IsGlobalNamespace,
	); err != nil {
		return nil, err
	}

	// first check if the name is already registered as the local namespace
	_, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: registerRequest.GetNamespace()})
	switch err.(type) {
	case nil:
		// namespace already exists, cannot proceed
		return nil, serviceerror.NewNamespaceAlreadyExistsf("Namespace %q already exists", registerRequest.GetNamespace())
	case *serviceerror.NamespaceNotFound:
		// namespace does not exists, proceeds
	default:
		// other err
		return nil, err
	}

	var activeClusterName string
	// input validation on cluster names
	if registerRequest.GetActiveClusterName() != "" {
		activeClusterName = registerRequest.GetActiveClusterName()
	} else {
		activeClusterName = d.clusterMetadata.GetCurrentClusterName()
	}
	var clusters []string
	for _, clusterConfig := range registerRequest.Clusters {
		clusterName := clusterConfig.GetClusterName()
		clusters = append(clusters, clusterName)
	}
	clusters = persistence.GetOrUseDefaultClusters(activeClusterName, clusters)

	currentHistoryArchivalState := namespace.NeverEnabledState()
	nextHistoryArchivalState := currentHistoryArchivalState
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(
			registerRequest.HistoryArchivalState,
			registerRequest.GetHistoryArchivalUri(),
			clusterHistoryArchivalConfig.GetNamespaceDefaultState(),
			clusterHistoryArchivalConfig.GetNamespaceDefaultURI(),
		)
		if err != nil {
			return nil, err
		}

		nextHistoryArchivalState, _, err = currentHistoryArchivalState.GetNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	currentVisibilityArchivalState := namespace.NeverEnabledState()
	nextVisibilityArchivalState := currentVisibilityArchivalState
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		archivalEvent, err := d.toArchivalRegisterEvent(
			registerRequest.VisibilityArchivalState,
			registerRequest.GetVisibilityArchivalUri(),
			clusterVisibilityArchivalConfig.GetNamespaceDefaultState(),
			clusterVisibilityArchivalConfig.GetNamespaceDefaultURI(),
		)
		if err != nil {
			return nil, err
		}

		nextVisibilityArchivalState, _, err = currentVisibilityArchivalState.GetNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	info := &persistencespb.NamespaceInfo{
		Id:          uuid.New(),
		Name:        registerRequest.GetNamespace(),
		State:       enumspb.NAMESPACE_STATE_REGISTERED,
		Owner:       registerRequest.GetOwnerEmail(),
		Description: registerRequest.GetDescription(),
		Data:        registerRequest.Data,
	}
	config := &persistencespb.NamespaceConfig{
		Retention:                    registerRequest.GetWorkflowExecutionRetentionPeriod(),
		HistoryArchivalState:         nextHistoryArchivalState.State,
		HistoryArchivalUri:           nextHistoryArchivalState.URI,
		VisibilityArchivalState:      nextVisibilityArchivalState.State,
		VisibilityArchivalUri:        nextVisibilityArchivalState.URI,
		BadBinaries:                  &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		CustomSearchAttributeAliases: nil,
	}
	replicationConfig := &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: activeClusterName,
		Clusters:          clusters,
		State:             enumspb.REPLICATION_STATE_NORMAL,
	}
	isGlobalNamespace := registerRequest.GetIsGlobalNamespace()

	if err := d.namespaceAttrValidator.ValidateNamespaceConfig(config); err != nil {
		return nil, err
	}
	if isGlobalNamespace {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForGlobalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	} else {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForLocalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	failoverVersion := common.EmptyVersion
	if registerRequest.GetIsGlobalNamespace() {
		failoverVersion = d.clusterMetadata.GetNextFailoverVersion(activeClusterName, 0)
	}

	namespaceRequest := &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:              info,
			Config:            config,
			ReplicationConfig: replicationConfig,
			ConfigVersion:     0,
			FailoverVersion:   failoverVersion,
		},
		IsGlobalNamespace: isGlobalNamespace,
	}

	namespaceResponse, err := d.metadataMgr.CreateNamespace(ctx, namespaceRequest)
	if err != nil {
		return nil, err
	}

	err = d.namespaceReplicator.HandleTransmissionTask(
		ctx,
		enumsspb.NAMESPACE_OPERATION_CREATE,
		namespaceRequest.Namespace.Info,
		namespaceRequest.Namespace.Config,
		namespaceRequest.Namespace.ReplicationConfig,
		false,
		namespaceRequest.Namespace.ConfigVersion,
		namespaceRequest.Namespace.FailoverVersion,
		namespaceRequest.IsGlobalNamespace,
		nil,
	)
	if err != nil {
		return nil, err
	}

	d.logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(registerRequest.GetNamespace()),
		tag.WorkflowNamespaceID(namespaceResponse.ID),
	)

	return &workflowservice.RegisterNamespaceResponse{}, nil
}

// ListNamespaces list all namespaces
func (d *namespaceHandler) ListNamespaces(
	ctx context.Context,
	listRequest *workflowservice.ListNamespacesRequest,
) (*workflowservice.ListNamespacesResponse, error) {

	pageSize := 100
	if listRequest.GetPageSize() != 0 {
		pageSize = int(listRequest.GetPageSize())
	}

	resp, err := d.metadataMgr.ListNamespaces(ctx, &persistence.ListNamespacesRequest{
		PageSize:       pageSize,
		NextPageToken:  listRequest.NextPageToken,
		IncludeDeleted: listRequest.GetNamespaceFilter().GetIncludeDeleted(),
	})

	if err != nil {
		return nil, err
	}

	var namespaces []*workflowservice.DescribeNamespaceResponse
	for _, namespace := range resp.Namespaces {
		desc := &workflowservice.DescribeNamespaceResponse{
			IsGlobalNamespace: namespace.IsGlobalNamespace,
			FailoverVersion:   namespace.Namespace.FailoverVersion,
		}
		desc.NamespaceInfo, desc.Config, desc.ReplicationConfig, desc.FailoverHistory =
			d.createResponse(
				namespace.Namespace.Info,
				namespace.Namespace.Config,
				namespace.Namespace.ReplicationConfig)
		namespaces = append(namespaces, desc)
	}

	response := &workflowservice.ListNamespacesResponse{
		Namespaces:    namespaces,
		NextPageToken: resp.NextPageToken,
	}

	return response, nil
}

// DescribeNamespace describe the namespace
func (d *namespaceHandler) DescribeNamespace(
	ctx context.Context,
	describeRequest *workflowservice.DescribeNamespaceRequest,
) (*workflowservice.DescribeNamespaceResponse, error) {

	// TODO, we should migrate the non global namespace to new table, see #773
	req := &persistence.GetNamespaceRequest{
		Name: describeRequest.GetNamespace(),
		ID:   describeRequest.GetId(),
	}
	resp, err := d.metadataMgr.GetNamespace(ctx, req)
	if err != nil {
		return nil, err
	}

	response := &workflowservice.DescribeNamespaceResponse{
		IsGlobalNamespace: resp.IsGlobalNamespace,
		FailoverVersion:   resp.Namespace.FailoverVersion,
	}
	response.NamespaceInfo, response.Config, response.ReplicationConfig, response.FailoverHistory =
		d.createResponse(resp.Namespace.Info, resp.Namespace.Config, resp.Namespace.ReplicationConfig)
	return response, nil
}

// UpdateNamespace update the namespace
//
//nolint:revive // cognitive complexity grandfathered
func (d *namespaceHandler) UpdateNamespace(
	ctx context.Context,
	updateRequest *workflowservice.UpdateNamespaceRequest,
) (*workflowservice.UpdateNamespaceResponse, error) {

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 namespace table
	// and since we do not know which table will return the namespace afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: updateRequest.GetNamespace()})
	if err != nil {
		return nil, err
	}

	info := getResponse.Namespace.Info
	config := getResponse.Namespace.Config
	replicationConfig := getResponse.Namespace.ReplicationConfig
	failoverHistory := getResponse.Namespace.ReplicationConfig.FailoverHistory
	configVersion := getResponse.Namespace.ConfigVersion
	failoverVersion := getResponse.Namespace.FailoverVersion
	failoverNotificationVersion := getResponse.Namespace.FailoverNotificationVersion
	isGlobalNamespace := getResponse.IsGlobalNamespace || updateRequest.PromoteNamespace
	needsNamespacePromotion := !getResponse.IsGlobalNamespace && updateRequest.PromoteNamespace

	currentHistoryArchivalState := &namespace.ArchivalConfigState{
		State: config.HistoryArchivalState,
		URI:   config.HistoryArchivalUri,
	}
	nextHistoryArchivalState := currentHistoryArchivalState
	historyArchivalConfigChanged := false
	clusterHistoryArchivalConfig := d.archivalMetadata.GetHistoryConfig()
	if updateRequest.Config != nil && clusterHistoryArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfig()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.HistoryArchivalState, cfg.GetHistoryArchivalUri(), clusterHistoryArchivalConfig.GetNamespaceDefaultURI())
		if err != nil {
			return nil, err
		}
		nextHistoryArchivalState, historyArchivalConfigChanged, err = currentHistoryArchivalState.GetNextState(archivalEvent, d.validateHistoryArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	currentVisibilityArchivalState := &namespace.ArchivalConfigState{
		State: config.VisibilityArchivalState,
		URI:   config.VisibilityArchivalUri,
	}
	nextVisibilityArchivalState := currentVisibilityArchivalState
	visibilityArchivalConfigChanged := false
	clusterVisibilityArchivalConfig := d.archivalMetadata.GetVisibilityConfig()
	if updateRequest.Config != nil && clusterVisibilityArchivalConfig.ClusterConfiguredForArchival() {
		cfg := updateRequest.GetConfig()
		archivalEvent, err := d.toArchivalUpdateEvent(cfg.VisibilityArchivalState, cfg.GetVisibilityArchivalUri(), clusterVisibilityArchivalConfig.GetNamespaceDefaultURI())
		if err != nil {
			return nil, err
		}
		nextVisibilityArchivalState, visibilityArchivalConfigChanged, err = currentVisibilityArchivalState.GetNextState(archivalEvent, d.validateVisibilityArchivalURI)
		if err != nil {
			return nil, err
		}
	}

	// whether active cluster is changed
	activeClusterChanged := false
	// whether anything other than active cluster is changed
	configurationChanged := false
	// whether replication cluster list is changed
	clusterListChanged := false

	if updateRequest.UpdateInfo != nil {
		updatedInfo := updateRequest.UpdateInfo
		if updatedInfo.GetDescription() != "" {
			configurationChanged = true
			info.Description = updatedInfo.GetDescription()
		}
		if updatedInfo.GetOwnerEmail() != "" {
			configurationChanged = true
			info.Owner = updatedInfo.GetOwnerEmail()
		}
		if updatedInfo.Data != nil {
			configurationChanged = true
			// only do merging
			info.Data = d.mergeNamespaceData(info.Data, updatedInfo.Data)
		}
		if updatedInfo.State != enumspb.NAMESPACE_STATE_UNSPECIFIED && info.State != updatedInfo.State {
			configurationChanged = true
			if err := validateStateUpdate(getResponse, updateRequest); err != nil {
				return nil, err
			}
			info.State = updatedInfo.State
		}
	}
	if updateRequest.Config != nil {
		updatedConfig := updateRequest.Config
		if updatedConfig.GetWorkflowExecutionRetentionTtl() != nil {
			configurationChanged = true

			config.Retention = updatedConfig.GetWorkflowExecutionRetentionTtl()
			if err := validateRetentionDuration(
				config.Retention,
				isGlobalNamespace,
			); err != nil {
				return nil, err
			}
		}
		if historyArchivalConfigChanged {
			configurationChanged = true
			config.HistoryArchivalState = nextHistoryArchivalState.State
			config.HistoryArchivalUri = nextHistoryArchivalState.URI
		}
		if visibilityArchivalConfigChanged {
			configurationChanged = true
			config.VisibilityArchivalState = nextVisibilityArchivalState.State
			config.VisibilityArchivalUri = nextVisibilityArchivalState.URI
		}
		if updatedConfig.BadBinaries != nil {
			maxLength := d.config.MaxBadBinaries(updateRequest.GetNamespace())
			// only do merging
			bb := d.mergeBadBinaries(config.BadBinaries.Binaries, updatedConfig.BadBinaries.Binaries, time.Now().UTC())
			config.BadBinaries = &bb
			if len(config.BadBinaries.Binaries) > maxLength {
				return nil, serviceerror.NewInvalidArgumentf("Total resetBinaries cannot exceed the max limit: %v", maxLength)
			}
		}
		if len(updatedConfig.CustomSearchAttributeAliases) > 0 {
			configurationChanged = true
			csaAliases, err := d.upsertCustomSearchAttributesAliases(
				config.CustomSearchAttributeAliases,
				updatedConfig.CustomSearchAttributeAliases,
			)
			if err != nil {
				return nil, err
			}
			config.CustomSearchAttributeAliases = csaAliases
		}
	}

	if updateRequest.GetDeleteBadBinary() != "" {
		binChecksum := updateRequest.GetDeleteBadBinary()
		_, ok := config.BadBinaries.Binaries[binChecksum]
		if !ok {
			return nil, serviceerror.NewInvalidArgumentf("Bad binary checksum %v doesn't exists.", binChecksum)
		}
		configurationChanged = true
		delete(config.BadBinaries.Binaries, binChecksum)
	}

	if updateRequest.ReplicationConfig != nil {
		updateReplicationConfig := updateRequest.ReplicationConfig
		if len(updateReplicationConfig.Clusters) != 0 {
			configurationChanged = true
			clusterListChanged = true
			var clustersNew []string
			for _, clusterConfig := range updateReplicationConfig.Clusters {
				clustersNew = append(clustersNew, clusterConfig.GetClusterName())
			}
			replicationConfig.Clusters = clustersNew
		}
		if updateReplicationConfig.State != enumspb.REPLICATION_STATE_UNSPECIFIED &&
			updateReplicationConfig.State != replicationConfig.State {
			if err := validateReplicationStateUpdate(getResponse, updateRequest); err != nil {
				return nil, err
			}
			configurationChanged = true
			replicationConfig.State = updateReplicationConfig.State
		}

		if updateReplicationConfig.GetActiveClusterName() != "" {
			activeClusterChanged = true
			replicationConfig.ActiveClusterName = updateReplicationConfig.GetActiveClusterName()
		}
	}

	if err := d.namespaceAttrValidator.ValidateNamespaceConfig(config); err != nil {
		return nil, err
	}
	if isGlobalNamespace {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForGlobalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
		if !d.clusterMetadata.IsGlobalNamespaceEnabled() {
			return nil, serviceerror.NewInvalidArgumentf("global namespace is not enabled on this "+
				"cluster, cannot update global namespace or promote local namespace: %v", updateRequest.Namespace)
		}
	} else {
		if err := d.namespaceAttrValidator.ValidateNamespaceReplicationConfigForLocalNamespace(
			replicationConfig,
		); err != nil {
			return nil, err
		}
	}

	if configurationChanged && activeClusterChanged && isGlobalNamespace {
		return nil, errCannotDoNamespaceFailoverAndUpdate
	} else if configurationChanged || activeClusterChanged || needsNamespacePromotion {
		if (needsNamespacePromotion || activeClusterChanged) && isGlobalNamespace {
			failoverVersion = d.clusterMetadata.GetNextFailoverVersion(
				replicationConfig.ActiveClusterName,
				failoverVersion,
			)
			failoverNotificationVersion = notificationVersion
		}
		// set the versions
		if configurationChanged {
			configVersion++
		}

		if (configurationChanged || activeClusterChanged) && isGlobalNamespace {
			// N.B., it should be sufficient to check only for activeClusterChanged. In order to be defensive, we also
			// check for configurationChanged. If nothing needs to be updated this will be a no-op.
			failoverHistory = d.maybeUpdateFailoverHistory(
				failoverHistory,
				updateRequest.ReplicationConfig,
				getResponse.Namespace,
				failoverVersion,
			)
		}

		replicationConfig.FailoverHistory = failoverHistory
		updateReq := &persistence.UpdateNamespaceRequest{
			Namespace: &persistencespb.NamespaceDetail{
				Info:                        info,
				Config:                      config,
				ReplicationConfig:           replicationConfig,
				ConfigVersion:               configVersion,
				FailoverVersion:             failoverVersion,
				FailoverNotificationVersion: failoverNotificationVersion,
			},
			IsGlobalNamespace:   isGlobalNamespace,
			NotificationVersion: notificationVersion,
		}
		err = d.metadataMgr.UpdateNamespace(ctx, updateReq)
		if err != nil {
			return nil, err
		}
	}

	err = d.namespaceReplicator.HandleTransmissionTask(
		ctx,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		info,
		config,
		replicationConfig,
		clusterListChanged,
		configVersion,
		failoverVersion,
		isGlobalNamespace,
		failoverHistory,
	)
	if err != nil {
		return nil, err
	}

	response := &workflowservice.UpdateNamespaceResponse{
		IsGlobalNamespace: isGlobalNamespace,
		FailoverVersion:   failoverVersion,
	}
	response.NamespaceInfo, response.Config, response.ReplicationConfig, _ = d.createResponse(info, config, replicationConfig)

	d.logger.Info("Update namespace succeeded",
		tag.WorkflowNamespace(info.Name),
		tag.WorkflowNamespaceID(info.Id),
	)
	return response, nil
}

// DeprecateNamespace deprecates a namespace
// Deprecated.
func (d *namespaceHandler) DeprecateNamespace(
	ctx context.Context,
	deprecateRequest *workflowservice.DeprecateNamespaceRequest,
) (*workflowservice.DeprecateNamespaceResponse, error) {

	clusterMetadata := d.clusterMetadata
	// TODO remove the IsGlobalNamespaceEnabled check once cross DC is public
	if clusterMetadata.IsGlobalNamespaceEnabled() && !clusterMetadata.IsMasterCluster() {
		return nil, errNotMasterCluster
	}

	// must get the metadata (notificationVersion) first
	// this version can be regarded as the lock on the v2 namespace table
	// and since we do not know which table will return the namespace afterwards
	// this call has to be made
	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	notificationVersion := metadata.NotificationVersion
	getResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: deprecateRequest.GetNamespace()})
	if err != nil {
		return nil, err
	}

	getResponse.Namespace.ConfigVersion = getResponse.Namespace.ConfigVersion + 1
	getResponse.Namespace.Info.State = enumspb.NAMESPACE_STATE_DEPRECATED
	updateReq := &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:                        getResponse.Namespace.Info,
			Config:                      getResponse.Namespace.Config,
			ReplicationConfig:           getResponse.Namespace.ReplicationConfig,
			ConfigVersion:               getResponse.Namespace.ConfigVersion,
			FailoverVersion:             getResponse.Namespace.FailoverVersion,
			FailoverNotificationVersion: getResponse.Namespace.FailoverNotificationVersion,
		},
		NotificationVersion: notificationVersion,
		IsGlobalNamespace:   getResponse.IsGlobalNamespace,
	}
	err = d.metadataMgr.UpdateNamespace(ctx, updateReq)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *namespaceHandler) CreateWorkflowRule(
	ctx context.Context,
	ruleSpec *rulespb.WorkflowRuleSpec,
	createdByIdentity string,
	description string,
	nsName string,
) (*rulespb.WorkflowRule, error) {

	if ruleSpec.GetId() == "" {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule ID is not set.")
	}

	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return nil, err
	}

	existingNamespace := getNamespaceResponse.Namespace
	config := getNamespaceResponse.Namespace.Config

	if config.WorkflowRules == nil {
		config.WorkflowRules = make(map[string]*rulespb.WorkflowRule)
	} else {
		maxRules := d.config.MaxWorkflowRulesPerNamespace(nsName)
		if len(config.WorkflowRules) >= maxRules {
			d.removeOldestExpiredWorkflowRule(nsName, config.WorkflowRules)
		}
		if len(config.WorkflowRules) >= maxRules {
			return nil, serviceerror.NewInvalidArgumentf("Workflow Rule limit exceeded. Max: %v", maxRules)
		}
	}

	_, ok := config.WorkflowRules[ruleSpec.GetId()]
	if ok {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule with this ID already exists.")
	}

	workflowRule := &rulespb.WorkflowRule{
		Spec:              ruleSpec,
		CreateTime:        timestamppb.New(d.timeSource.Now()),
		CreatedByIdentity: createdByIdentity,
		Description:       description,
	}
	config.WorkflowRules[ruleSpec.GetId()] = workflowRule

	updateReq := &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:                        existingNamespace.Info,
			Config:                      config,
			ReplicationConfig:           existingNamespace.ReplicationConfig,
			ConfigVersion:               existingNamespace.ConfigVersion + 1,
			FailoverVersion:             existingNamespace.FailoverVersion,
			FailoverNotificationVersion: existingNamespace.FailoverNotificationVersion,
		},
		IsGlobalNamespace:   getNamespaceResponse.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}
	err = d.metadataMgr.UpdateNamespace(ctx, updateReq)
	if err != nil {
		return nil, err
	}

	return workflowRule, nil
}

func (d *namespaceHandler) removeOldestExpiredWorkflowRule(nsName string, rules map[string]*rulespb.WorkflowRule) {
	oldestTime := d.timeSource.Now()
	var oldestKey string
	found := false

	for key, rule := range rules {
		expirationTime := rule.GetSpec().GetExpirationTime()
		if expirationTime == nil {
			continue
		}
		if !found || expirationTime.AsTime().Before(oldestTime) {
			oldestTime = expirationTime.AsTime()
			oldestKey = key
			found = true
		}
	}

	if found {
		d.logger.Info(
			"Removed expired workflow rule",
			tag.WorkflowRuleID(oldestKey),
			tag.WorkflowNamespace(nsName),
		)
		delete(rules, oldestKey)
	}
}

func (d *namespaceHandler) DescribeWorkflowRule(
	ctx context.Context, ruleID string, nsName string,
) (*rulespb.WorkflowRule, error) {
	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return nil, err
	}

	if getNamespaceResponse.Namespace.Config.WorkflowRules == nil {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}

	rule, ok := getNamespaceResponse.Namespace.Config.WorkflowRules[ruleID]
	if !ok {
		return nil, serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}

	return rule, nil
}

func (d *namespaceHandler) DeleteWorkflowRule(
	ctx context.Context, ruleID string, nsName string,
) error {
	if ruleID == "" {
		return serviceerror.NewInvalidArgument("Workflow Rule ID is not set.")
	}

	metadata, err := d.metadataMgr.GetMetadata(ctx)
	if err != nil {
		return err
	}

	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return err
	}

	existingNamespace := getNamespaceResponse.Namespace
	config := getNamespaceResponse.Namespace.Config
	if config.WorkflowRules == nil {
		return serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}
	_, ok := config.WorkflowRules[ruleID]
	if !ok {
		return serviceerror.NewInvalidArgument("Workflow Rule with this ID not Found.")
	}

	delete(config.WorkflowRules, ruleID)

	updateReq := &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:                        existingNamespace.Info,
			Config:                      config,
			ReplicationConfig:           existingNamespace.ReplicationConfig,
			ConfigVersion:               existingNamespace.ConfigVersion + 1,
			FailoverVersion:             existingNamespace.FailoverVersion,
			FailoverNotificationVersion: existingNamespace.FailoverNotificationVersion,
		},
		IsGlobalNamespace:   getNamespaceResponse.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}
	return d.metadataMgr.UpdateNamespace(ctx, updateReq)
}

func (d *namespaceHandler) ListWorkflowRules(
	ctx context.Context, nsName string,
) ([]*rulespb.WorkflowRule, error) {
	getNamespaceResponse, err := d.metadataMgr.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: nsName})
	if err != nil {
		return nil, err
	}

	workflowRulesMap := getNamespaceResponse.Namespace.Config.WorkflowRules
	if workflowRulesMap == nil {
		return []*rulespb.WorkflowRule{}, nil
	}

	workflowRules := make([]*rulespb.WorkflowRule, 0, len(workflowRulesMap))
	for _, rule := range workflowRulesMap {
		workflowRules = append(workflowRules, rule)
	}
	return workflowRules, nil
}

func (d *namespaceHandler) createResponse(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	replicationConfig *persistencespb.NamespaceReplicationConfig,
) (*namespacepb.NamespaceInfo, *namespacepb.NamespaceConfig, *replicationpb.NamespaceReplicationConfig, []*replicationpb.FailoverStatus) {

	infoResult := &namespacepb.NamespaceInfo{
		Name:        info.Name,
		State:       info.State,
		Description: info.Description,
		OwnerEmail:  info.Owner,
		Data:        info.Data,
		Id:          info.Id,

		Capabilities: &namespacepb.NamespaceInfo_Capabilities{
			EagerWorkflowStart: d.config.EnableEagerWorkflowStart(info.Name),
			SyncUpdate:         d.config.EnableUpdateWorkflowExecution(info.Name),
			AsyncUpdate:        d.config.EnableUpdateWorkflowExecutionAsyncAccepted(info.Name),
		},
		SupportsSchedules: d.config.EnableSchedules(info.Name),
	}

	configResult := &namespacepb.NamespaceConfig{
		WorkflowExecutionRetentionTtl: config.Retention,
		HistoryArchivalState:          config.HistoryArchivalState,
		HistoryArchivalUri:            config.HistoryArchivalUri,
		VisibilityArchivalState:       config.VisibilityArchivalState,
		VisibilityArchivalUri:         config.VisibilityArchivalUri,
		BadBinaries:                   config.BadBinaries,
		CustomSearchAttributeAliases:  config.CustomSearchAttributeAliases,
	}

	var clusters []*replicationpb.ClusterReplicationConfig
	for _, cluster := range replicationConfig.Clusters {
		clusters = append(clusters, &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster,
		})
	}
	replicationConfigResult := &replicationpb.NamespaceReplicationConfig{
		ActiveClusterName: replicationConfig.ActiveClusterName,
		Clusters:          clusters,
	}

	var failoverHistory []*replicationpb.FailoverStatus
	for _, entry := range replicationConfig.GetFailoverHistory() {
		failoverHistory = append(failoverHistory, &replicationpb.FailoverStatus{
			FailoverTime:    entry.GetFailoverTime(),
			FailoverVersion: entry.GetFailoverVersion(),
		})
	}

	return infoResult, configResult, replicationConfigResult, failoverHistory
}

func (d *namespaceHandler) mergeBadBinaries(
	old map[string]*namespacepb.BadBinaryInfo,
	new map[string]*namespacepb.BadBinaryInfo,
	createTime time.Time,
) namespacepb.BadBinaries {

	if old == nil {
		old = map[string]*namespacepb.BadBinaryInfo{}
	}
	for k, v := range new {
		v.CreateTime = timestamppb.New(createTime)
		old[k] = v
	}
	return namespacepb.BadBinaries{
		Binaries: old,
	}
}

func (d *namespaceHandler) mergeNamespaceData(
	old map[string]string,
	new map[string]string,
) map[string]string {

	if old == nil {
		old = map[string]string{}
	}
	for k, v := range new {
		old[k] = v
	}
	return old
}

func (d *namespaceHandler) upsertCustomSearchAttributesAliases(
	current map[string]string,
	upsert map[string]string,
) (map[string]string, error) {
	result := util.CloneMapNonNil(current)
	for key, value := range upsert {
		if value == "" {
			delete(result, key)
		} else if _, ok := current[key]; !ok {
			result[key] = value
		} else {
			return nil, errCustomSearchAttributeFieldAlreadyAllocated
		}
	}
	return result, nil
}

func (d *namespaceHandler) toArchivalRegisterEvent(
	state enumspb.ArchivalState,
	URI string,
	defaultState enumspb.ArchivalState,
	defaultURI string,
) (*namespace.ArchivalConfigEvent, error) {

	event := &namespace.ArchivalConfigEvent{
		State:      state,
		URI:        URI,
		DefaultURI: defaultURI,
	}
	if event.State == enumspb.ARCHIVAL_STATE_UNSPECIFIED {
		event.State = defaultState
	}
	if err := event.Validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *namespaceHandler) toArchivalUpdateEvent(
	state enumspb.ArchivalState,
	URI string,
	defaultURI string,
) (*namespace.ArchivalConfigEvent, error) {

	event := &namespace.ArchivalConfigEvent{
		State:      state,
		URI:        URI,
		DefaultURI: defaultURI,
	}
	if err := event.Validate(); err != nil {
		return nil, err
	}
	return event, nil
}

func (d *namespaceHandler) validateHistoryArchivalURI(URIString string) error {
	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return err
	}

	a, err := d.archiverProvider.GetHistoryArchiver(URI.Scheme())
	if err != nil {
		return err
	}

	return a.ValidateURI(URI)
}

func (d *namespaceHandler) validateVisibilityArchivalURI(URIString string) error {
	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return err
	}

	a, err := d.archiverProvider.GetVisibilityArchiver(URI.Scheme())
	if err != nil {
		return err
	}

	return a.ValidateURI(URI)
}

// maybeUpdateFailoverHistory adds an entry if the Namespace is becoming active in a new cluster.
func (d *namespaceHandler) maybeUpdateFailoverHistory(
	failoverHistory []*persistencespb.FailoverStatus,
	updateReplicationConfig *replicationpb.NamespaceReplicationConfig,
	namespaceDetail *persistencespb.NamespaceDetail,
	newFailoverVersion int64,
) []*persistencespb.FailoverStatus {
	d.logger.Debug(
		"maybeUpdateFailoverHistory",
		tag.NewAnyTag("failoverHistory", failoverHistory),
		tag.NewAnyTag("updateReplConfig", updateReplicationConfig),
		tag.NewAnyTag("namespaceDetail", namespaceDetail),
	)
	if updateReplicationConfig == nil {
		d.logger.Debug("updateReplicationConfig was nil")
		return failoverHistory
	}

	lastFailoverVersion := int64(-1)
	if l := len(namespaceDetail.ReplicationConfig.FailoverHistory); l > 0 {
		lastFailoverVersion = namespaceDetail.ReplicationConfig.FailoverHistory[l-1].FailoverVersion
	}
	if lastFailoverVersion != newFailoverVersion {
		now := d.timeSource.Now()
		failoverHistory = append(
			failoverHistory, &persistencespb.FailoverStatus{
				FailoverTime:    timestamppb.New(now),
				FailoverVersion: newFailoverVersion,
			},
		)
	}
	if l := len(failoverHistory); l > maxReplicationHistorySize {
		failoverHistory = failoverHistory[l-maxReplicationHistorySize : l]
	}
	return failoverHistory
}

// validateRetentionDuration ensures that retention duration can't be set below a sane minimum.
func validateRetentionDuration(retention *durationpb.Duration, isGlobalNamespace bool) error {
	if err := timestamp.ValidateAndCapProtoDuration(retention); err != nil {
		return errInvalidRetentionPeriod
	}

	min := namespace.MinRetentionLocal
	if isGlobalNamespace {
		min = namespace.MinRetentionGlobal
	}
	if timestamp.DurationValue(retention) < min {
		return errInvalidRetentionPeriod
	}
	return nil
}

func validateReplicationStateUpdate(existingNamespace *persistence.GetNamespaceResponse, nsUpdateRequest *workflowservice.UpdateNamespaceRequest) error {
	if nsUpdateRequest.ReplicationConfig == nil ||
		nsUpdateRequest.ReplicationConfig.State == enumspb.REPLICATION_STATE_UNSPECIFIED ||
		nsUpdateRequest.ReplicationConfig.State == existingNamespace.Namespace.ReplicationConfig.State {
		return nil // no change
	}

	if existingNamespace.Namespace.Info.State != enumspb.NAMESPACE_STATE_REGISTERED {
		return serviceerror.NewInvalidArgumentf(
			"update ReplicationState is only supported when namespace is in %s state, current state: %s",
			enumspb.NAMESPACE_STATE_REGISTERED,
			existingNamespace.Namespace.Info.State,
		)
	}

	if nsUpdateRequest.ReplicationConfig.State == enumspb.REPLICATION_STATE_HANDOVER {
		if !existingNamespace.IsGlobalNamespace {
			return serviceerror.NewInvalidArgumentf(
				"%s can only be set for global namespace",
				enumspb.REPLICATION_STATE_HANDOVER,
			)
		}
		// verify namespace has more than 1 replication clusters
		replicationClusterCount := len(existingNamespace.Namespace.ReplicationConfig.Clusters)
		if len(nsUpdateRequest.ReplicationConfig.Clusters) > 0 {
			replicationClusterCount = len(nsUpdateRequest.ReplicationConfig.Clusters)
		}
		if replicationClusterCount < 2 {
			return serviceerror.NewInvalidArgumentf("%s require more than one replication clusters", enumspb.REPLICATION_STATE_HANDOVER)
		}
	}
	return nil
}

func validateStateUpdate(existingNamespace *persistence.GetNamespaceResponse, nsUpdateRequest *workflowservice.UpdateNamespaceRequest) error {
	if nsUpdateRequest.UpdateInfo == nil {
		return nil // no change
	}
	oldState := existingNamespace.Namespace.Info.State
	newState := nsUpdateRequest.UpdateInfo.State
	if newState == enumspb.NAMESPACE_STATE_UNSPECIFIED || oldState == newState {
		return nil // no change
	}

	if existingNamespace.Namespace.ReplicationConfig != nil &&
		existingNamespace.Namespace.ReplicationConfig.State == enumspb.REPLICATION_STATE_HANDOVER {
		return serviceerror.NewInvalidArgument("cannot update namespace state while its replication state in REPLICATION_STATE_HANDOVER")
	}

	switch oldState {
	case enumspb.NAMESPACE_STATE_REGISTERED:
		switch newState {
		case enumspb.NAMESPACE_STATE_DELETED, enumspb.NAMESPACE_STATE_DEPRECATED:
			return nil
		default:
			return errInvalidNamespaceStateUpdate
		}

	case enumspb.NAMESPACE_STATE_DEPRECATED:
		switch newState {
		case enumspb.NAMESPACE_STATE_DELETED:
			return nil
		default:
			return errInvalidNamespaceStateUpdate
		}

	case enumspb.NAMESPACE_STATE_DELETED:
		return errInvalidNamespaceStateUpdate
	default:
		return errInvalidNamespaceStateUpdate
	}
}
