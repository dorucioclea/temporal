package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
)

func TestForceReplicationWorkflow(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 4}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	totalPageCount := 4
	currentPageCount := 0
	layout := "2006-01-01 00:00Z"
	startTime, _ := time.Parse(layout, "2020-01-01 00:00Z")
	closeTime, _ := time.Parse(layout, "2020-02-01 00:00Z")

	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []*commonpb.WorkflowExecution{{WorkflowId: "wf-1"}},
				NextPageToken: []byte("fake-page-token"),
				LastStartTime: startTime,
				LastCloseTime: closeTime,
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []*commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
			LastStartTime: startTime,
			LastCloseTime: closeTime,
		}, nil
	}).Times(totalPageCount)
	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil).Times(totalPageCount)
	env.OnActivity(a.VerifyReplicationTasks, mock.Anything, mock.Anything).Return(verifyReplicationTasksResponse{VerifiedWorkflowCount: 1}, nil).Times(totalPageCount)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Times(1)
	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 100,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
		EnableVerification:      true,
		TargetClusterEndpoint:   "test-target",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)

	var status ForceReplicationStatus
	err = envValue.Get(&status)
	require.NoError(t, err)
	assert.Equal(t, 0, status.ContinuedAsNewCount)
	assert.Equal(t, startTime, status.LastStartTime)
	assert.Equal(t, closeTime, status.LastCloseTime)
	assert.True(t, status.TaskQueueUserDataReplicationStatus.Done)
	assert.Equal(t, "", status.TaskQueueUserDataReplicationStatus.FailureMessage)
	assert.Equal(t, int64(4), status.TotalWorkflowCount)
	assert.Equal(t, int64(4), status.ReplicatedWorkflowCount)
	assert.Equal(t, []byte(nil), status.PageTokenForRestart)
}

func TestForceReplicationWorkflow_ContinueAsNew(t *testing.T) {
	totalPageCount := 4
	currentPageCount := 0
	testMaxPageCountPerExecution := 2
	layout := "2006-01-01 00:00Z"
	startTime, _ := time.Parse(layout, "2020-01-01 00:00Z")
	closeTime, _ := time.Parse(layout, "2020-02-01 00:00Z")

	mockListWorkflows := func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []*commonpb.WorkflowExecution{},
				NextPageToken: []byte(fmt.Sprintf("fake-page-token-%d", currentPageCount)),
				LastStartTime: startTime,
				LastCloseTime: closeTime,
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []*commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
		}, nil
	}

	expectedContinueAsNewParams := ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		GetParentInfoRPS:        2.0,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   testMaxPageCountPerExecution,
		NextPageToken:           []byte("fake-page-token-2"),
		EnableVerification:      true,
		TargetClusterEndpoint:   "test-target",
		TargetClusterName:       "",
		VerifyIntervalInSeconds: defaultVerifyIntervalInSeconds,
		LastCloseTime:           closeTime,
		LastStartTime:           startTime,
		ContinuedAsNewCount:     1,
		TaskQueueUserDataReplicationParams: TaskQueueUserDataReplicationParams{
			PageSize: 0,
			RPS:      0,
		},
		ReplicatedWorkflowCount:          0,
		TotalForceReplicateWorkflowCount: 10,
	}

	expectContinueAsNew := true

	// Run the workflow once. We should get a continue as new error.
	continueAsNewInput, queryStatus := testRunForceReplicationForContinueAsNew(t,
		mockListWorkflows,
		ForceReplicationParams{
			Namespace:               "test-ns",
			Query:                   "",
			ConcurrentActivityCount: 2,
			OverallRps:              10,
			ListWorkflowsPageSize:   1,
			PageCountPerExecution:   testMaxPageCountPerExecution,
			EnableVerification:      true,
			TargetClusterEndpoint:   "test-target",
			NextPageToken:           []byte("fake-initial-page-token"),
		},
		expectContinueAsNew,
		testMaxPageCountPerExecution,
	)
	require.NotNil(t, continueAsNewInput)

	// tl;dr We do not check TaskQueueUserDataReplicationStatus because we do not know if
	// ForceTaskQueueUserDataReplicationWorkflow will complete in a given execution of
	// force replication.
	//
	// ForceTaskQueueUserDataReplicationWorkflow is a child workflow that runs in parallel
	// and may span many ContinueAsNew'd executions of force replication:
	//
	// - It is started on the first execution as a child workflow (when ContinueAsNewCount == 0).
	// - It is not started on subsequent executions (when ContinueAsNewCount > 0)
	// - Only the final execution waits for it to complete (when NextPageToken == nil)
	//
	// To keep this test simple and to resolve past test flakes, we do not test all cases.
	// We test with ContinueAsNewCount > 0 and NextPageToken != nil, so that:
	//
	// - ForceTaskQueueUserDataReplicationWorkflow is not started
	// - Force replication does not get stuck waiting for some previous execution of
	//   ForceTaskQueueUserDataReplicationWorkflow to finish
	//
	// Another test checks that ForceTaskQueueUserDataReplicationWorkflow is invoked correctly.
	require.Empty(t, cmp.Diff(
		expectedContinueAsNewParams, *continueAsNewInput,
		cmpopts.IgnoreFields(ForceReplicationParams{}, "TaskQueueUserDataReplicationStatus"),
		cmpopts.IgnoreFields(ForceReplicationParams{}, "EstimationMultiplier"),
		cmpopts.IgnoreFields(ForceReplicationParams{}, "QPSQueue"),
	))

	assert.Equal(t, closeTime, queryStatus.LastCloseTime)
	assert.Equal(t, startTime, queryStatus.LastStartTime)
	assert.Equal(t, 1, queryStatus.ContinuedAsNewCount)
	assert.Equal(t, []byte("fake-initial-page-token"), queryStatus.PageTokenForRestart)
}

func testRunForceReplicationForContinueAsNew(t *testing.T,
	mockListWorkflows func(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error),
	input ForceReplicationParams,
	expectContinueAsNew bool,
	expMaxPageCountPerExecution int,
) (*ForceReplicationParams, ForceReplicationStatus) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	namespaceID := uuid.New()

	var a *activities
	if input.TotalForceReplicateWorkflowCount == 0 {
		env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 10}, nil)
	}
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(mockListWorkflows).Times(expMaxPageCountPerExecution)
	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil).Times(expMaxPageCountPerExecution)
	env.OnActivity(a.VerifyReplicationTasks, mock.Anything, mock.Anything).Return(verifyReplicationTasksResponse{}, nil).Times(expMaxPageCountPerExecution)
	// ForceTaskQueueUserDataReplicationWorkflow runs in parallel as a child and may span many ContinueAsNew'd
	// executions of ForceReplication. The SeedReplicationQueueWithUserDataEntries activity will eventually run
	// once, but we aren't guaranteed that it will run during any given execution of ForceReplication.
	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil).Maybe()
	env.ExecuteWorkflow(ForceReplicationWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	env.AssertExpectations(t)

	var continueAsNewErr *workflow.ContinueAsNewError
	var continueAsNewParams *ForceReplicationParams
	if !expectContinueAsNew {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		require.True(t, errors.As(err, &continueAsNewErr))

		var params ForceReplicationParams
		payloads := continueAsNewErr.Input.GetPayloads()
		require.Len(t, payloads, 1)
		require.NoError(t, json.Unmarshal(payloads[0].GetData(), &params))
		continueAsNewParams = &params
	}

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)

	var status ForceReplicationStatus
	require.NoError(t, envValue.Get(&status))

	return continueAsNewParams, status
}

func TestForceReplicationWorkflow_InvalidInput(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}

	for _, invalidInput := range []ForceReplicationParams{
		{
			// Empty namespace
		},
		{
			// Empty TargetClusterEndpoint
			Namespace:          uuid.New(),
			EnableVerification: true,
		},
	} {
		env := testSuite.NewTestWorkflowEnvironment()
		env.ExecuteWorkflow(ForceReplicationWorkflow, invalidInput)

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.Error(t, err)
		require.ErrorContains(t, err, "InvalidArgument")
		env.AssertExpectations(t)

	}
}

func TestForceReplicationWorkflow_ListWorkflowsError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 10}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	maxPageCountPerExecution := 2
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(nil, errors.New("mock listWorkflows error"))

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   maxPageCountPerExecution,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock listWorkflows error")
	env.AssertExpectations(t)
}

func TestForceReplicationWorkflow_GenerateReplicationTaskRetryableError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 10}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	totalPageCount := 4
	currentPageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []*commonpb.WorkflowExecution{},
				NextPageToken: []byte("fake-page-token"),
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []*commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
		}, nil
	}).Times(totalPageCount)

	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(errors.New("mock generate replication tasks error"))

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock generate replication tasks error")
	env.AssertExpectations(t)
}

func TestForceReplicationWorkflow_GenerateReplicationTaskNonRetryableError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 10}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	totalPageCount := 4
	currentPageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []*commonpb.WorkflowExecution{},
				NextPageToken: []byte("fake-page-token"),
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []*commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
		}, nil
	})

	var errMsg = "mock generate replication tasks error"
	// Only expect GenerateReplicationTasks to execute once and workflow will then fail because of
	// non-retryable error.
	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(
		temporal.NewNonRetryableApplicationError(errMsg, "", nil),
	).Times(1)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 1,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
		EnableVerification:      true,
		TargetClusterEndpoint:   "test-target",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), errMsg)
	env.AssertExpectations(t)
}

func TestForceReplicationWorkflow_VerifyReplicationTaskNonRetryableError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 10}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	totalPageCount := 4
	currentPageCount := 0
	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
		assert.Equal(t, "test-ns", request.Namespace)
		currentPageCount++
		if currentPageCount < totalPageCount {
			return &listWorkflowsResponse{
				Executions:    []*commonpb.WorkflowExecution{},
				NextPageToken: []byte("fake-page-token"),
			}, nil
		}
		// your mock function implementation
		return &listWorkflowsResponse{
			Executions:    []*commonpb.WorkflowExecution{},
			NextPageToken: nil, // last page
		}, nil
	})

	var errMsg = "mock verify replication tasks error"
	// GenerateReplicationTasks and VerifyReplicationTasks runs in paralle. GenerateReplicationTasks may not start before VerifyReplicationTasks failed.
	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil).Maybe()
	env.OnActivity(a.VerifyReplicationTasks, mock.Anything, mock.Anything).Return(
		verifyReplicationTasksResponse{},
		temporal.NewNonRetryableApplicationError(errMsg, "", nil),
	).Times(1)

	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 1,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   4,
		EnableVerification:      true,
		TargetClusterEndpoint:   "test-target",
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), errMsg)
	env.AssertExpectations(t)
}

func TestForceReplicationWorkflow_TaskQueueReplicationFailure(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.CountWorkflow, mock.Anything, mock.Anything).Return(&countWorkflowResponse{WorkflowCount: 10}, nil)
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	env.OnActivity(a.ListWorkflows, mock.Anything, mock.Anything).Return(&listWorkflowsResponse{
		Executions:    []*commonpb.WorkflowExecution{},
		NextPageToken: nil, // last page
	}, nil)
	env.OnActivity(a.GenerateReplicationTasks, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.SeedReplicationQueueWithUserDataEntries, mock.Anything, mock.Anything).Return(
		temporal.NewNonRetryableApplicationError("namespace is required", "InvalidArgument", nil),
	)

	env.ExecuteWorkflow(ForceReplicationWorkflow, ForceReplicationParams{
		Namespace:               "test-ns",
		Query:                   "",
		ConcurrentActivityCount: 2,
		OverallRps:              10,
		ListWorkflowsPageSize:   1,
		PageCountPerExecution:   maxPageCountPerExecution,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)

	envValue, err := env.QueryWorkflow(forceReplicationStatusQueryType)
	require.NoError(t, err)

	var status ForceReplicationStatus
	err = envValue.Get(&status)
	require.NoError(t, err)
	assert.True(t, status.TaskQueueUserDataReplicationStatus.Done)
	assert.Contains(t, status.TaskQueueUserDataReplicationStatus.FailureMessage, "namespace is required")
	assert.Equal(t, []byte(nil), status.PageTokenForRestart)
}

func TestSeedReplicationQueueWithUserDataEntries_Heartbeats(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	namespaceID := uuid.New()

	ctrl := gomock.NewController(t)
	mockFrontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	mockTaskManager := persistence.NewMockTaskManager(ctrl)
	mockNamespaceReplicationQueue := persistence.NewMockNamespaceReplicationQueue(ctrl)
	a := &activities{
		namespaceReplicationQueue: mockNamespaceReplicationQueue,
		taskManager:               mockTaskManager,
		frontendClient:            mockFrontendClient,
		logger:                    log.NewCLILogger(),
	}

	// Once per attempt
	mockFrontendClient.EXPECT().DescribeNamespace(gomock.Any(), gomock.Any()).Times(2).Return(&workflowservice.DescribeNamespaceResponse{NamespaceInfo: &namespacepb.NamespaceInfo{Id: namespaceID}}, nil)
	// Twice for the first page due to expected failure of the first activity attempt, once for the second page
	mockTaskManager.EXPECT().ListTaskQueueUserDataEntries(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(
		func(ctx context.Context, request *persistence.ListTaskQueueUserDataEntriesRequest) (*persistence.ListTaskQueueUserDataEntriesResponse, error) {
			if len(request.NextPageToken) == 0 {
				return &persistence.ListTaskQueueUserDataEntriesResponse{
					NextPageToken: []byte{0xac, 0xdc},
					Entries:       []*persistence.TaskQueueUserDataEntry{{TaskQueue: "a"}, {TaskQueue: "b"}},
				}, nil
			}
			return &persistence.ListTaskQueueUserDataEntriesResponse{
				NextPageToken: []byte{},
				Entries:       make([]*persistence.TaskQueueUserDataEntry, 0),
			}, nil
		},
	)

	numCalls := 0
	mockNamespaceReplicationQueue.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(func(ctx context.Context, task *replicationspb.ReplicationTask) error {
		assert.Equal(t, namespaceID, task.GetTaskQueueUserDataAttributes().NamespaceId)
		numCalls++
		if numCalls == 1 {
			assert.Equal(t, "a", task.GetTaskQueueUserDataAttributes().TaskQueueName)
		} else {
			// b is published twice
			assert.Equal(t, "b", task.GetTaskQueueUserDataAttributes().TaskQueueName)
		}
		if numCalls == 2 {
			return errors.New("some random error")
		}
		return nil
	})
	iceptor := heartbeatRecordingInterceptor{T: t}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&iceptor}})
	env.RegisterActivity(a)
	params := TaskQueueUserDataReplicationParamsWithNamespace{
		TaskQueueUserDataReplicationParams: TaskQueueUserDataReplicationParams{PageSize: 10, RPS: 1},
		Namespace:                          "foo",
	}
	_, err := env.ExecuteActivity(a.SeedReplicationQueueWithUserDataEntries, params)
	assert.Error(t, err)
	assert.Equal(t, len(iceptor.seedRecordedHeartbeats), 2)
	assert.Equal(t, []byte(nil), iceptor.seedRecordedHeartbeats[1].NextPageToken)
	assert.Equal(t, 1, iceptor.seedRecordedHeartbeats[1].IndexInPage)
	env.SetHeartbeatDetails(iceptor.seedRecordedHeartbeats[1])
	_, err = env.ExecuteActivity(a.SeedReplicationQueueWithUserDataEntries, params)
	assert.NoError(t, err)
}

// The SDK's test environment throttles emitted heartbeat forcing us to use an interceptor to record the heartbeat details
type heartbeatRecordingInterceptor struct {
	interceptor.WorkerInterceptorBase
	interceptor.ActivityInboundInterceptorBase
	interceptor.ActivityOutboundInterceptorBase
	seedRecordedHeartbeats                []seedReplicationQueueWithUserDataEntriesHeartbeatDetails
	replicationRecordedHeartbeats         []replicationTasksHeartbeatDetails
	generateReplicationRecordedHeartbeats []int
	T                                     *testing.T
}

func (i *heartbeatRecordingInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	i.ActivityInboundInterceptorBase.Next = next
	return i
}

func (i *heartbeatRecordingInterceptor) Init(outbound interceptor.ActivityOutboundInterceptor) error {
	i.ActivityOutboundInterceptorBase.Next = outbound
	return i.ActivityInboundInterceptorBase.Init(i)
}

func (i *heartbeatRecordingInterceptor) RecordHeartbeat(ctx context.Context, details ...interface{}) {
	if d, ok := details[0].(seedReplicationQueueWithUserDataEntriesHeartbeatDetails); ok {
		i.seedRecordedHeartbeats = append(i.seedRecordedHeartbeats, d)
	} else if d, ok := details[0].(replicationTasksHeartbeatDetails); ok {
		i.replicationRecordedHeartbeats = append(i.replicationRecordedHeartbeats, d)
	} else if d, ok := details[0].(int); ok {
		i.generateReplicationRecordedHeartbeats = append(i.generateReplicationRecordedHeartbeats, d)
	} else {
		assert.Fail(i.T, "invalid heartbeat details")
	}

	i.ActivityOutboundInterceptorBase.Next.RecordHeartbeat(ctx, details...)
}
