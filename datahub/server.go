package datahub

import (
	"fmt"
	"net"

	datahub_metric_v1alpha2 "github.com/containers-ai/api/datahub/metric/v1alpha2"
	datahub_prediction_v1alpha2 "github.com/containers-ai/api/datahub/prediction/v1alpha2"
	datahub_resource_v1alpha2 "github.com/containers-ai/api/datahub/resource/v1alpha2"
	datahub_score_v1alpha2 "github.com/containers-ai/api/datahub/score/v1alpha2"
	datahub_v1alpha2 "github.com/containers-ai/api/datahub/v1alpha2"
	"github.com/containers-ai/karina/datahub/pkg/dao"
	cluster_status_dao "github.com/containers-ai/karina/datahub/pkg/dao/cluster_status"
	cluster_status_dao_impl "github.com/containers-ai/karina/datahub/pkg/dao/cluster_status/impl"
	metric_dao "github.com/containers-ai/karina/datahub/pkg/dao/metric"
	prometheusMetricDAO "github.com/containers-ai/karina/datahub/pkg/dao/metric/prometheus"
	prediction_dao "github.com/containers-ai/karina/datahub/pkg/dao/prediction"
	prediction_dao_impl "github.com/containers-ai/karina/datahub/pkg/dao/prediction/impl"
	recommendation_dao "github.com/containers-ai/karina/datahub/pkg/dao/recommendation"
	recommendation_dao_impl "github.com/containers-ai/karina/datahub/pkg/dao/recommendation/impl"
	"github.com/containers-ai/karina/datahub/pkg/dao/score"
	"github.com/containers-ai/karina/datahub/pkg/dao/score/impl/influxdb"
	"github.com/containers-ai/karina/operator/pkg/apis"
	autoscaling_v1alpha1 "github.com/containers-ai/karina/operator/pkg/apis/autoscaling/v1alpha1"
	recommendation_reconciler "github.com/containers-ai/karina/operator/pkg/reconciler/recommendation"
	"github.com/containers-ai/karina/pkg/utils/log"
	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type Server struct {
	err    chan error
	server *grpc.Server

	Config    Config
	K8SClient client.Client
}

var (
	scope = log.RegisterScope("gRPC", "gRPC server log", 0)
)

// NewServer returns Server instance
func NewServer(cfg Config) (*Server, error) {
	var (
		err error

		server *Server
		k8sCli client.Client
	)

	if err = cfg.Validate(); err != nil {
		return server, errors.New("Configuration validation failed: " + err.Error())
	}
	k8sClientConfig, err := config.GetConfig()
	if err != nil {
		return server, errors.New("Get kubernetes configuration failed: " + err.Error())
	}

	if k8sCli, err = client.New(k8sClientConfig, client.Options{}); err != nil {
		return server, errors.New("Create kubernetes client failed: " + err.Error())
	}

	mgr, err := manager.New(k8sClientConfig, manager.Options{})
	if err != nil {
		scope.Error(err.Error())
	}
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		scope.Error(err.Error())
	}

	server = &Server{
		err: make(chan error),

		Config:    cfg,
		K8SClient: k8sCli,
	}

	return server, nil
}

func (s *Server) Run() error {

	// build server listener
	scope.Info(("starting gRPC server"))
	ln, err := net.Listen("tcp", s.Config.BindAddress)
	if err != nil {
		scope.Error("gRPC server failed listen: " + err.Error())
		return fmt.Errorf("GRPC server failed to bind address: %s", s.Config.BindAddress)
	}
	scope.Info("gRPC server listening on " + s.Config.BindAddress)

	server, err := s.newGRPCServer()
	if err != nil {
		scope.Error(err.Error())
		return err
	}
	s.server = server

	s.registGRPCServer(server)
	reflection.Register(server)

	if err := server.Serve(ln); err != nil {
		s.err <- fmt.Errorf("GRPC server failed to serve: %s", err.Error())
	}

	return nil
}

func (s *Server) Stop() error {

	s.server.Stop()

	return nil
}

func (s *Server) Err() <-chan error {
	return s.err
}

func (s *Server) newGRPCServer() (*grpc.Server, error) {

	var (
		server *grpc.Server
	)

	server = grpc.NewServer()

	return server, nil
}

func (s *Server) registGRPCServer(server *grpc.Server) {

	datahub_v1alpha2.RegisterDatahubServiceServer(server, s)
}

// ListPodMetrics list pods' metrics
func (s *Server) ListPodMetrics(ctx context.Context, in *datahub_v1alpha2.ListPodMetricsRequest) (*datahub_v1alpha2.ListPodMetricsResponse, error) {

	var (
		err error

		metricDAO metric_dao.MetricsDAO

		requestExt     datahubListPodMetricsRequestExtended
		namespace      = ""
		podName        = ""
		queryCondition dao.QueryCondition

		podsMetricMap     metric_dao.PodsMetricMap
		datahubPodMetrics []*datahub_metric_v1alpha2.PodMetric
	)

	requestExt = datahubListPodMetricsRequestExtended{*in}
	if err = requestExt.validate(); err != nil {
		return &datahub_v1alpha2.ListPodMetricsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INVALID_ARGUMENT),
				Message: err.Error(),
			},
		}, nil
	}

	metricDAO = prometheusMetricDAO.NewWithConfig(*s.Config.Prometheus)

	if in.GetNamespacedName() != nil {
		namespace = in.GetNamespacedName().GetNamespace()
		podName = in.GetNamespacedName().GetName()
	}
	queryCondition = datahubQueryConditionExtend{queryCondition: in.GetQueryCondition()}.metricDAOQueryCondition()
	listPodMetricsRequest := metric_dao.ListPodMetricsRequest{
		Namespace:      namespace,
		PodName:        podName,
		QueryCondition: queryCondition,
	}

	podsMetricMap, err = metricDAO.ListPodMetrics(listPodMetricsRequest)
	if err != nil {
		scope.Errorf("ListPodMetrics failed: %+v", errors.Cause(err))
		errMsg := "Internal server error"
		switch err.(type) {
		case metric_dao.ErrorQueryConditionExceedMaximum:
			errMsg = errorQueryConditionExceedMaximum
		default:
		}
		return &datahub_v1alpha2.ListPodMetricsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: errMsg,
			},
		}, nil
	}

	for _, podMetric := range podsMetricMap {
		podMetricExtended := daoPodMetricExtended{podMetric}
		datahubPodMetric := podMetricExtended.datahubPodMetric()
		datahubPodMetrics = append(datahubPodMetrics, datahubPodMetric)
	}

	return &datahub_v1alpha2.ListPodMetricsResponse{
		Status: &status.Status{
			Code: int32(code.Code_OK),
		},
		PodMetrics: datahubPodMetrics,
	}, nil
}

// ListNodeMetrics list nodes' metrics
func (s *Server) ListNodeMetrics(ctx context.Context, in *datahub_v1alpha2.ListNodeMetricsRequest) (*datahub_v1alpha2.ListNodeMetricsResponse, error) {

	var (
		err error

		metricDAO metric_dao.MetricsDAO

		requestExt     datahubListNodeMetricsRequestExtended
		nodeNames      []string
		queryCondition dao.QueryCondition

		nodesMetricMap     metric_dao.NodesMetricMap
		datahubNodeMetrics []*datahub_metric_v1alpha2.NodeMetric
	)

	requestExt = datahubListNodeMetricsRequestExtended{*in}
	if err = requestExt.validate(); err != nil {
		return &datahub_v1alpha2.ListNodeMetricsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INVALID_ARGUMENT),
				Message: err.Error(),
			},
		}, nil
	}

	metricDAO = prometheusMetricDAO.NewWithConfig(*s.Config.Prometheus)

	nodeNames = in.GetNodeNames()
	queryCondition = datahubQueryConditionExtend{queryCondition: in.GetQueryCondition()}.metricDAOQueryCondition()
	listNodeMetricsRequest := metric_dao.ListNodeMetricsRequest{
		NodeNames:      nodeNames,
		QueryCondition: queryCondition,
	}

	nodesMetricMap, err = metricDAO.ListNodesMetric(listNodeMetricsRequest)
	if err != nil {
		scope.Errorf("ListNodeMetrics failed: %+v", errors.Cause(err))
		errMsg := "Internal server error"
		switch err.(type) {
		case metric_dao.ErrorQueryConditionExceedMaximum:
			errMsg = errorQueryConditionExceedMaximum
		}
		return &datahub_v1alpha2.ListNodeMetricsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: errMsg,
			},
		}, nil
	}

	for _, nodeMetric := range nodesMetricMap {
		nodeMetricExtended := daoNodeMetricExtended{nodeMetric}
		datahubNodeMetric := nodeMetricExtended.datahubNodeMetric()
		datahubNodeMetrics = append(datahubNodeMetrics, datahubNodeMetric)
	}

	return &datahub_v1alpha2.ListNodeMetricsResponse{
		Status: &status.Status{
			Code: int32(code.Code_OK),
		},
		NodeMetrics: datahubNodeMetrics,
	}, nil
}

// ListPods returns predicted pods
func (s *Server) ListPods(ctx context.Context, in *datahub_v1alpha2.ListPodsRequest) (*datahub_v1alpha2.ListPodsResponse, error) {
	var containerDAO cluster_status_dao.ContainerOperation = &cluster_status_dao_impl.Container{
		InfluxDBConfig: *s.Config.InfluxDB,
	}
	scalerNS, scalerName := "", ""
	if scaler := in.GetScaler(); scaler != nil {
		scalerNS = scaler.GetNamespace()
		scalerName = scaler.GetName()
	}

	if pods, err := containerDAO.ListPredictedPods(scalerNS, scalerName, in.GetIsPredicted()); err != nil {
		scope.Errorf("ListPod failed: %+v", err.Error())
		return &datahub_v1alpha2.ListPodsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: err.Error(),
			},
		}, nil
	} else {
		return &datahub_v1alpha2.ListPodsResponse{
			Pods: pods,
			Status: &status.Status{
				Code: int32(code.Code_OK),
			},
		}, nil
	}
}

// ListNodes list nodes in cluster
func (s *Server) ListNodes(ctx context.Context, in *datahub_v1alpha2.ListNodesRequest) (*datahub_v1alpha2.ListNodesResponse, error) {
	var nodeDAO cluster_status_dao.NodeOperation = &cluster_status_dao_impl.Node{
		InfluxDBConfig: *s.Config.InfluxDB,
	}

	if predictedNodes, err := nodeDAO.ListNodes(in.GetIsPredicted()); err != nil {
		scope.Errorf("ListNodes failed: %+v", errors.Cause(err))
		return &datahub_v1alpha2.ListNodesResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: err.Error(),
			},
		}, nil
	} else {
		return &datahub_v1alpha2.ListNodesResponse{
			Status: &status.Status{
				Code: int32(code.Code_OK),
			},
			Nodes: predictedNodes,
		}, nil
	}
}

// ListPodPredictions list pods' predictions
func (s *Server) ListPodPredictions(ctx context.Context, in *datahub_v1alpha2.ListPodPredictionsRequest) (*datahub_v1alpha2.ListPodPredictionsResponse, error) {

	var (
		err error

		predictionDAO prediction_dao.DAO

		podsPredicitonMap     *prediction_dao.PodsPredictionMap
		datahubPodPredicitons []*datahub_prediction_v1alpha2.PodPrediction

		apiResponseInternalServerError = datahub_v1alpha2.ListPodPredictionsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: "Internal server error.",
			},
		}
	)

	predictionDAO = prediction_dao_impl.NewInfluxDBWithConfig(*s.Config.InfluxDB)

	datahubListPodPredictionsRequestExtended := datahubListPodPredictionsRequestExtended{in}
	listPodPredictionsRequest := datahubListPodPredictionsRequestExtended.daoListPodPredictionsRequest()
	podsPredicitonMap, err = predictionDAO.ListPodPredictions(listPodPredictionsRequest)
	if err != nil {
		scope.Errorf("ListPodPrediction failed: %+v", errors.Cause(err))
		return &apiResponseInternalServerError, nil
	}

	for _, ptrPodPrediction := range *podsPredicitonMap {
		podPredicitonExtended := daoPtrPodPredictionExtended{ptrPodPrediction}
		datahubPodPrediction := podPredicitonExtended.datahubPodPrediction()
		datahubPodPredicitons = append(datahubPodPredicitons, datahubPodPrediction)
	}

	return &datahub_v1alpha2.ListPodPredictionsResponse{
		Status: &status.Status{
			Code: int32(code.Code_OK),
		},
		PodPredictions: datahubPodPredicitons,
	}, nil
}

// ListNodePredictions list nodes' predictions
func (s *Server) ListNodePredictions(ctx context.Context, in *datahub_v1alpha2.ListNodePredictionsRequest) (*datahub_v1alpha2.ListNodePredictionsResponse, error) {

	var (
		err error

		predictionDAO prediction_dao.DAO

		nodesPredicitonMap     *prediction_dao.NodesPredictionMap
		datahubNodePredicitons []*datahub_prediction_v1alpha2.NodePrediction

		apiResponseInternalServerError = datahub_v1alpha2.ListNodePredictionsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: "Internal server error.",
			},
		}
	)

	predictionDAO = prediction_dao_impl.NewInfluxDBWithConfig(*s.Config.InfluxDB)

	datahubListNodePredictionsRequestExtended := datahubListNodePredictionsRequestExtended{in}
	listNodePredictionRequest := datahubListNodePredictionsRequestExtended.daoListNodePredictionsRequest()
	nodesPredicitonMap, err = predictionDAO.ListNodePredictions(listNodePredictionRequest)
	if err != nil {
		scope.Errorf("ListNodePredictions failed: %+v", errors.Cause(err))
		return &apiResponseInternalServerError, nil
	}

	datahubNodePredicitons = daoPtrNodesPredictionMapExtended{nodesPredicitonMap}.datahubNodePredictions()

	return &datahub_v1alpha2.ListNodePredictionsResponse{
		Status: &status.Status{
			Code: int32(code.Code_OK),
		},
		NodePredictions: datahubNodePredicitons,
	}, nil
}

// ListPodRecommendations list pod recommendations
func (s *Server) ListPodRecommendations(ctx context.Context, in *datahub_v1alpha2.ListPodRecommendationsRequest) (*datahub_v1alpha2.ListPodRecommendationsResponse, error) {
	var containerDAO recommendation_dao.ContainerOperation = &recommendation_dao_impl.Container{
		InfluxDBConfig: *s.Config.InfluxDB,
	}

	if podRecommendations, err := containerDAO.ListPodRecommendations(in.GetNamespacedName(), in.GetQueryCondition()); err != nil {
		scope.Errorf("ListPodRecommendations failed: %+v", errors.Cause(err))
		return &datahub_v1alpha2.ListPodRecommendationsResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: err.Error(),
			},
		}, nil
	} else {
		return &datahub_v1alpha2.ListPodRecommendationsResponse{
			Status: &status.Status{
				Code: int32(code.Code_OK),
			},
			PodRecommendations: podRecommendations,
		}, nil
	}
}

// ListSimulatedSchedulingScores list simulated scheduling scores
func (s *Server) ListSimulatedSchedulingScores(ctx context.Context, in *datahub_v1alpha2.ListSimulatedSchedulingScoresRequest) (*datahub_v1alpha2.ListSimulatedSchedulingScoresResponse, error) {

	var (
		err error

		scoreDAO                          score.DAO
		scoreDAOListRequest               score.ListRequest
		scoreDAOSimulatedSchedulingScores = make([]*score.SimulatedSchedulingScore, 0)

		datahubScores = make([]*datahub_score_v1alpha2.SimulatedSchedulingScore, 0)
	)

	scoreDAO = influxdb.NewWithConfig(*s.Config.InfluxDB)

	datahubListSimulatedSchedulingScoresRequestExtended := datahubListSimulatedSchedulingScoresRequestExtended{in}
	scoreDAOListRequest = datahubListSimulatedSchedulingScoresRequestExtended.daoLisRequest()

	scoreDAOSimulatedSchedulingScores, err = scoreDAO.ListSimulatedScheduingScores(scoreDAOListRequest)
	if err != nil {
		scope.Errorf("ListSimulatedSchedulingScores failed: %+v", errors.Cause(err))
		return &datahub_v1alpha2.ListSimulatedSchedulingScoresResponse{
			Status: &status.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: "Internal server error.",
			},
			Scores: datahubScores,
		}, nil
	}

	for _, daoSimulatedSchedulingScore := range scoreDAOSimulatedSchedulingScores {

		t, err := ptypes.TimestampProto(daoSimulatedSchedulingScore.Timestamp)
		if err != nil {
			scope.Warnf("api ListSimulatedSchedulingScores warn: time convert failed: %s", err.Error())
		}
		datahubScore := datahub_score_v1alpha2.SimulatedSchedulingScore{
			Time:        t,
			ScoreBefore: daoSimulatedSchedulingScore.ScoreBefore,
			ScoreAfter:  daoSimulatedSchedulingScore.ScoreAfter,
		}
		datahubScores = append(datahubScores, &datahubScore)
	}

	return &datahub_v1alpha2.ListSimulatedSchedulingScoresResponse{
		Status: &status.Status{
			Code: int32(code.Code_OK),
		},
		Scores: datahubScores,
	}, nil
}

// UpdateNodes list nodes in cluster
func (s *Server) UpdateNodes(ctx context.Context, in *datahub_v1alpha2.UpdateNodesRequest) (*status.Status, error) {
	var nodeDAO cluster_status_dao.NodeOperation = &cluster_status_dao_impl.Node{
		InfluxDBConfig: *s.Config.InfluxDB,
	}

	if err := nodeDAO.UpdateNodes(in.GetUpdatedNodes()); err != nil {
		scope.Errorf("UpdateNodes failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: "Internal server error.",
		}, nil
	}
	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// UpdatePods add containers information of pods to database
func (s *Server) UpdatePods(ctx context.Context, in *datahub_v1alpha2.UpdatePodsRequest) (*status.Status, error) {
	var containerDAO cluster_status_dao.ContainerOperation = &cluster_status_dao_impl.Container{
		InfluxDBConfig: *s.Config.InfluxDB,
	}
	if err := containerDAO.UpdatePods(in.GetUpdatedPods()); err != nil {
		scope.Errorf("UpdatePods failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: "Internal server error.",
		}, nil
	}
	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// CreatePods add containers information of pods to database
func (s *Server) CreatePods(ctx context.Context, in *datahub_v1alpha2.CreatePodsRequest) (*status.Status, error) {
	var containerDAO cluster_status_dao.ContainerOperation = &cluster_status_dao_impl.Container{
		InfluxDBConfig: *s.Config.InfluxDB,
	}

	if err := containerDAO.AddPods(in.GetPods()); err != nil {
		scope.Errorf("CreatePods failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: err.Error(),
		}, nil
	}
	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// CreateNodes add node information to database
func (s *Server) CreateNodes(ctx context.Context, in *datahub_v1alpha2.CreateNodesRequest) (*status.Status, error) {
	var nodeDAO cluster_status_dao.NodeOperation = &cluster_status_dao_impl.Node{
		InfluxDBConfig: *s.Config.InfluxDB,
	}
	if err := nodeDAO.AddNodes(in.GetNodes()); err != nil {
		scope.Errorf("CreateNodes failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: err.Error(),
		}, nil
	}

	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// CreatePodPredictions add pod predictions information to database
func (s *Server) CreatePodPredictions(ctx context.Context, in *datahub_v1alpha2.CreatePodPredictionsRequest) (*status.Status, error) {

	var (
		err error

		predictionDAO        prediction_dao.DAO
		containersPrediciton []*prediction_dao.ContainerPrediction

		apiResponseInternalServerError = status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: "Internal server error.",
		}
	)

	predictionDAO = prediction_dao_impl.NewInfluxDBWithConfig(*s.Config.InfluxDB)

	containersPrediciton = datahubCreatePodPredictionsRequestExtended{*in}.daoContainerPredictions()
	err = predictionDAO.CreateContainerPredictions(containersPrediciton)
	if err != nil {
		scope.Errorf("CreatePodPredictions failed: %+v", errors.Cause(err))
		return &apiResponseInternalServerError, nil
	}

	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// CreateNodePredictions add node predictions information to database
func (s *Server) CreateNodePredictions(ctx context.Context, in *datahub_v1alpha2.CreateNodePredictionsRequest) (*status.Status, error) {

	var (
		err error

		predictionDAO   prediction_dao.DAO
		nodesPrediciton []*prediction_dao.NodePrediction

		apiResponseInternalServerError = status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: "Internal server error.",
		}
	)

	predictionDAO = prediction_dao_impl.NewInfluxDBWithConfig(*s.Config.InfluxDB)

	nodesPrediciton = datahubCreateNodePredictionsRequestExtended{*in}.daoNodePredictions()
	err = predictionDAO.CreateNodePredictions(nodesPrediciton)
	if err != nil {
		return &apiResponseInternalServerError, nil
	}

	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// CreatePodRecommendations add pod recommendations information to database
func (s *Server) CreatePodRecommendations(ctx context.Context, in *datahub_v1alpha2.CreatePodRecommendationsRequest) (*status.Status, error) {
	var containerDAO recommendation_dao.ContainerOperation = &recommendation_dao_impl.Container{
		InfluxDBConfig: *s.Config.InfluxDB,
	}

	podRecommendations := in.GetPodRecommendations()
	for _, podRecommendation := range podRecommendations {
		podNS := podRecommendation.GetNamespacedName().Namespace
		podName := podRecommendation.GetNamespacedName().Name
		recommendation := &autoscaling_v1alpha1.Recommendation{}

		if err := s.K8SClient.Get(context.TODO(), types.NamespacedName{
			Namespace: podNS,
			Name:      podName,
		}, recommendation); err == nil {
			recommendationReconciler := recommendation_reconciler.NewReconciler(s.K8SClient, recommendation)
			if recommendation, err = recommendationReconciler.UpdateResourceRecommendation(podRecommendation); err == nil {
				if err = s.K8SClient.Update(context.TODO(), recommendation); err != nil {
					scope.Error(err.Error())
				}
			}
		} else if !k8s_errors.IsNotFound(err) {
			scope.Error(err.Error())
		}
	}

	if err := containerDAO.AddPodRecommendations(podRecommendations); err != nil {
		scope.Errorf("CreatePodRecommendations failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: err.Error(),
		}, err
	}

	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// CreateSimulatedSchedulingScores add simulated scheduling scores to database
func (s *Server) CreateSimulatedSchedulingScores(ctx context.Context, in *datahub_v1alpha2.CreateSimulatedSchedulingScoresRequest) (*status.Status, error) {

	var (
		err error

		scoreDAO                           score.DAO
		daoSimulatedSchedulingScoreEntites = make([]*score.SimulatedSchedulingScore, 0)
	)

	scoreDAO = influxdb.NewWithConfig(*s.Config.InfluxDB)

	for _, scoreEntity := range in.GetScores() {

		if scoreEntity == nil {
			continue
		}

		timestamp, _ := ptypes.Timestamp(scoreEntity.GetTime())
		daoSimulatedSchedulingScoreEntity := score.SimulatedSchedulingScore{
			Timestamp:   timestamp,
			ScoreBefore: float64(scoreEntity.GetScoreBefore()),
			ScoreAfter:  float64(scoreEntity.GetScoreAfter()),
		}
		daoSimulatedSchedulingScoreEntites = append(daoSimulatedSchedulingScoreEntites, &daoSimulatedSchedulingScoreEntity)
	}

	err = scoreDAO.CreateSimulatedScheduingScores(daoSimulatedSchedulingScoreEntites)
	if err != nil {
		scope.Errorf("CreateSimulatedSchedulingScores failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: "Internal server error.",
		}, nil
	}

	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// DeleteNodes remove node information to database
func (s *Server) DeleteNodes(ctx context.Context, in *datahub_v1alpha2.DeleteNodesRequest) (*status.Status, error) {
	var nodeDAO cluster_status_dao.NodeOperation = &cluster_status_dao_impl.Node{
		InfluxDBConfig: *s.Config.InfluxDB,
	}
	nodeList := []*datahub_resource_v1alpha2.Node{}
	for _, predictedNode := range in.GetNodes() {
		nodeList = append(nodeList, &datahub_resource_v1alpha2.Node{
			Name: predictedNode.GetName(),
		})
	}
	if err := nodeDAO.DeleteNodes(nodeList); err != nil {
		scope.Errorf("DeleteNodes failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: err.Error(),
		}, nil
	}

	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}

// DeletePods update containers information of pods to database
func (s *Server) DeletePods(ctx context.Context, in *datahub_v1alpha2.DeletePodsRequest) (*status.Status, error) {
	var containerDAO cluster_status_dao.ContainerOperation = &cluster_status_dao_impl.Container{
		InfluxDBConfig: *s.Config.InfluxDB,
	}
	if err := containerDAO.DeletePods(in.GetPods()); err != nil {
		scope.Errorf("DeletePods failed: %+v", errors.Cause(err))
		return &status.Status{
			Code:    int32(code.Code_INTERNAL),
			Message: "Internal server error.",
		}, nil
	}
	return &status.Status{
		Code: int32(code.Code_OK),
	}, nil
}
