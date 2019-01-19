package recommendation

type containerTag string
type containerField string

const (
	// ContainerTime is the time to apply recommendation
	ContainerTime containerTag = "time"
	// ContainerNamespace is recommended container namespace
	ContainerNamespace containerTag = "namespace"
	// ContainerName is recommended container name
	ContainerName containerTag = "name"
	// ContainerPodName is pod name of recommended container
	ContainerPodName containerTag = "pod_name"

	// ContainerPolicy is recommended CPU request
	ContainerPolicy containerField = "policy"
	// ContainerResourceRequestCPU is recommended CPU request
	ContainerResourceRequestCPU containerField = "resource_request_cpu"
	// ContainerResourceRequestMemory is recommended memory request
	ContainerResourceRequestMemory containerField = "resource_request_memory"
	// ContainerResourceLimitCPU is recommended CPU limit
	ContainerResourceLimitCPU containerField = "resource_limit_cpu"
	// ContainerResourceLimitMemory is recommended memory limit
	ContainerResourceLimitMemory containerField = "resource_limit_memory"
	// ContainerInitialResourceRequestCPU is recommended initial CPU request
	ContainerInitialResourceRequestCPU containerField = "initial_resource_request_cpu"
	// ContainerInitialResourceRequestMemory is recommended initial memory request
	ContainerInitialResourceRequestMemory containerField = "initial_resource_request_memory"
	// ContainerInitialResourceLimitCPU is recommended initial CPU limit
	ContainerInitialResourceLimitCPU containerField = "initial_resource_limit_cpu"
	// ContainerInitialResourceLimitMemory is recommended initial memory limit
	ContainerInitialResourceLimitMemory containerField = "initial_resource_limit_memory"
)

var (
	// ContainerTags is list of tags of karina_container_recommendation measurement
	ContainerTags = []containerTag{
		ContainerTime,
		ContainerNamespace,
		ContainerName,
		ContainerPodName,
	}
	// ContainerFields is list of fields of karina_container_recommendation measurement
	ContainerFields = []containerField{
		ContainerPolicy,
		ContainerResourceRequestCPU,
		ContainerResourceRequestMemory,
		ContainerResourceLimitCPU,
		ContainerResourceLimitMemory,
		ContainerInitialResourceRequestCPU,
		ContainerInitialResourceRequestMemory,
		ContainerInitialResourceLimitCPU,
		ContainerInitialResourceLimitMemory,
	}
)
