package tests

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func (s *FunctionalSuite) TestUpdateWorkflow_ExampleUpdateTest() {
	// Create sdk client and worker
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	s.NoError(err)
	sdkWorker := worker.New(sdkClient, "my-tq", worker.Options{})

	// Define and register a workflow with an update handler
	workflowFn := func(wfCtx workflow.Context) (string, error) {
		l := workflow.GetLogger(wfCtx)

		var updateArgs []string

		workflow.SetUpdateHandlerWithOptions(wfCtx, "my-update-handler",
			func(arg string) (string, error) {
				l.Info("In update handler")
				updateArgs = append(updateArgs, arg)
				return arg + "-result", nil
			},
			workflow.UpdateHandlerOptions{
				Validator: func(arg string) error {
					l.Info("In update validator")
					return nil
				},
			})

		workflow.Await(wfCtx, func() bool { return len(updateArgs) > 0 })

		return "wf-result", nil
	}
	sdkWorker.RegisterWorkflow(workflowFn)
	s.NoError(sdkWorker.Start())
	defer sdkWorker.Stop()

	// Start a workflow and send an update
	ctx := context.Background()
	wfHandle, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "my-wfid",
		TaskQueue: "my-tq",
	}, workflowFn)
	s.NoError(err)

	updateHandle, err := sdkClient.UpdateWorkflow(ctx, "my-wfid", wfHandle.GetRunID(), "my-update-handler", "my-update-arg")
	s.NoError(err)

	var updateResult string
	s.NoError(updateHandle.Get(ctx, &updateResult))
	fmt.Println("update result", updateResult)

	var wfResult string
	s.NoError(wfHandle.Get(ctx, &wfResult))
	fmt.Println("wf result", wfResult)
}
