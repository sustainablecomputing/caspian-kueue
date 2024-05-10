// Note: the example only works with the code within the same release/branch.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/lanl/clp"
	core "github.com/sustainablecomputing/caspian/core"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/scheme"
	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type Scheduler struct {
	N            int // number of  jobs (AWs) (to be scheduled/rescheduled)
	M            int // number of available clusters
	T            int // length of time horizon (number of timeslots)
	PeriodLength int
	Jobs         []core.Job     // list of jobs
	Clusters     []core.Cluster // spoke clusters
	crdClient    *rest.RESTClient
	crdClient2   *rest.RESTClient
	clientset    *kubernetes.Clientset
}

type SchedulingDecision struct {
	i    int     //job index
	j    int     //cluster index
	t    int     //time slot
	xbar float64 //allocation by lp
}

type Objective struct { //
	j   int
	t   int
	val float64
}

var WLResource = schema.GroupVersionResource{Group: kueuev1beta1.GroupVersion.Group,
	Version: kueuev1beta1.GroupVersion.Version, Resource: "workloads"}
var JobResource = schema.GroupVersionResource{Group: batchv1.GroupName,
	Version: batchv1.SchemeGroupVersion.Version, Resource: "jobs"}

// NewDispatcher : create a new dispather instance and
// configure the clients with kube_config and hub-context
func NewScheduler(config *rest.Config, periodLength int) *Scheduler {
	s := &Scheduler{
		N:            0,
		M:            0,
		T:            core.DefaultT,
		PeriodLength: periodLength,
		Jobs:         []core.Job{},
		Clusters:     []core.Cluster{},
		crdClient:    &rest.RESTClient{},
		crdClient2:   &rest.RESTClient{},
	}
	//config, err := buildConfigWithContextFromFlags(hub_contxt, kube_config)
	crdConfig := *config
	crdConfig2 := *config
	crdConfig.ContentConfig.GroupVersion = &schema.GroupVersion{Group: kueuev1beta1.GroupVersion.Group,
		Version: kueuev1beta1.GroupVersion.Version}
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	crdConfig.UserAgent = rest.DefaultKubernetesUserAgent()

	crdConfig2.ContentConfig.GroupVersion = &schema.GroupVersion{Group: batchv1.GroupName,
		Version: batchv1.SchemeGroupVersion.Version}
	crdConfig2.APIPath = "/apis"
	crdConfig2.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	crdConfig2.UserAgent = rest.DefaultKubernetesUserAgent()

	var err error
	s.crdClient, err = rest.UnversionedRESTClientFor(&crdConfig)
	s.crdClient2, err = rest.UnversionedRESTClientFor(&crdConfig2)
	if err != nil {
		panic(err)
	}

	s.clientset, err = kubernetes.NewForConfig(config)
	return s
}

// retrive all Appwrappers in hub: running+ non-running AWs.
// Calculate requested resources  of each AW.
// Save all AWs with their characteristics in Jobs array
func (s *Scheduler) GetJobs(count int) {
	//cc := 0.0

	jobs := batchv1.JobList{}
	s.Jobs = []core.Job{}

	err := s.crdClient2.
		Get().
		Resource("jobs").
		Do(context.Background()).
		Into(&jobs)
	if err != nil {
		panic(err)
	}

	fmt.Println("\nList of Jobs ")
	fmt.Println("Name\t CPU\t GPU\t RemainTime  \t Deadline \t \t  Status ")
	var jobNum int
	jobNum = 0
	stopPolicy := kueuev1beta1.Hold //"Hold"
	if len(jobs.Items) > 0 {
		err := s.UpdateQueuePoloicy("cq-1", stopPolicy)
		if err != nil {
			panic(err)
		}
	}
	for _, job := range jobs.Items {

		if len(job.Spec.Template.Spec.NodeSelector) == 0 {
			jobNum = jobNum + 1
			fmt.Println(job.Name)
			choice := rand.IntN(3)
			if jobNum <= 1 {
				if choice < 1 {
					err = s.AddNodeSelector(job.Name, job.Namespace, "high") //workloads.Items[0].Name
					fmt.Println(err, job.Name, " scheduled on node groupe with carbon=high")
				} else {
					err = s.AddNodeSelector(job.Name, job.Namespace, "low")
					fmt.Println(err, job.Name, " scheduled on node groupe with carbon=low")
				}
			} else {
				if count > 1 {
					if choice < 1 {
						err = s.AddNodeSelector(job.Name, job.Namespace, "high") //workloads.Items[0].Name
						fmt.Println(err, job.Name, " scheduled on node groupe with carbon=high")
					} else {
						err = s.AddNodeSelector(job.Name, job.Namespace, "low")
						fmt.Println(err, job.Name, " scheduled on node groupe with carbon=low")
					}
				}
			}

		}
	}
	stopPolicy = kueuev1beta1.None
	if len(jobs.Items) > 0 {
		err := s.UpdateQueuePoloicy("cq-1", stopPolicy)
		if err != nil {
			panic(err)
		}
	}
}
func (s *Scheduler) UpdateQueuePoloicy(Name string, Policy kueuev1beta1.StopPolicy) error {

	patch := []interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/spec/stopPolicy",
			"value": Policy,
		},
	}

	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	err = s.crdClient.Patch(types.JSONPatchType).Resource("clusterqueues").Name(Name).Body(payload).Do(context.Background()).Error()

	return err
}

func (s *Scheduler) AddNodeSelector(Name string, NameSpace string, targetNode string) error {

	var a [1]map[string]string
	a[0] = map[string]string{"carbon": targetNode}

	patch := []interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/spec/template/spec/nodeSelector",
			"value": a[0],
		},
	}

	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	err = s.crdClient2.Patch(types.JSONPatchType).Resource("jobs").Name(Name).Namespace(NameSpace).Body(payload).Do(context.Background()).Error()

	return err

}

func (s *Scheduler) Schedule(optimizer string, count int) {
	s.GetJobs(count)

}

func (s *Scheduler) Optimize(sustainable bool) []int {

	N := s.N
	M := s.M
	T := s.T
	omega1 := 0.0
	omega2 := 1.0
	omega3 := 1.0
	theta1 := 1.0
	theta2 := 1.0
	theta3 := 1.0
	obj1 := make([]float64, N*M*T)
	obj2 := make([]float64, N*M*T)
	obj3 := make([]float64, N*M*T)
	obj := make([]float64, N*M*T)
	Available_GPU := make([]float64, M*T)
	Available_CPU := make([]float64, M*T)
	Targets := make([]int, N)
	Objectives := make([][]Objective, N)

	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {

				lateness := float64(t) + float64(s.Jobs[i].RemainTime-s.Jobs[i].Deadline)
				if lateness <= 0 {
					lateness = 1
				}

				obj1[i*M*T+j*T+t] = 0

				for tt := t; tt < min(s.T, t+int(s.Jobs[i].RemainTime)); tt++ {
					obj1[i*T*M+j*T+t] += (s.Jobs[i].GPU) / (s.Clusters[j].GPU * s.Clusters[j].Carbon[tt] * s.Clusters[j].PowerSlope)
				}

				obj2[i*M*T+j*T+t] = 1 / (float64(t) + float64(s.Jobs[i].RemainTime))
				obj3[i*M*T+j*T+t] = 1 / lateness

				//fmt.Println(obj1[i*T*M+j*T+t], obj2[i*T*M+j*T+t], "fff")
			}
		}
	}

	if sustainable {

		omega1 = 0
		omega2 = 1
		omega2 = 3
		_, theta1 = s.LPSolve(obj1)
		_, theta2 = s.LPSolve(obj2)
		_, theta3 = s.LPSolve(obj3)
		//fmt.Println(theta1, theta2, "lll")
	}
	for i := 0; i < N; i++ {
		cnt := 0
		Objectives[i] = make([]Objective, M*T)
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				if sustainable {
					obj[i*M*T+j*T+t] = omega1*obj1[i*M*T+j*T+t]/theta1 + omega2*obj2[i*M*T+j*T+t]/theta2 + omega3*obj3[i*M*T+j*T+t]/theta3
				} else {
					obj[i*M*T+j*T+t] = omega2*obj2[i*M*T+j*T+t] + omega3*obj3[i*M*T+j*T+t]/theta3

				}
				//fmt.Println(i, j, t, omega1*obj1[i*M*T+j*T+t]/theta1, omega2*obj2[i*M*T+j*T+t]/theta2, "kkk")
				Objectives[i][cnt].j = j
				Objectives[i][cnt].t = t
				Objectives[i][cnt].val = obj[i*T*M+j*T+t]
				Available_GPU[j*T+t] = s.Clusters[j].GPU
				Available_CPU[j*T+t] = s.Clusters[j].CPU
				cnt++
			}
		}
		sort.Slice(Objectives[i], func(h, k int) bool {
			return Objectives[i][h].val > Objectives[i][k].val
		})
	}

	SchedulingDecisions, _ := s.LPSolve(obj)

	sort.Slice(SchedulingDecisions, func(i, j int) bool {
		return SchedulingDecisions[i].xbar > SchedulingDecisions[j].xbar
	})

	for ii := 0; ii < N; ii++ {

		i := SchedulingDecisions[ii].i
		j := SchedulingDecisions[ii].j
		t := SchedulingDecisions[ii].t
		Targets[i] = -1
		found := false
		if SchedulingDecisions[ii].xbar >= 1 {

			found = true
		} else {
			for cnt := 0; cnt < M*T; cnt++ {
				j = Objectives[i][cnt].j
				t = Objectives[i][cnt].t

				found = true
				for tt := t; tt < min(s.T, t+int(s.Jobs[i].RemainTime)); tt++ {
					if Available_GPU[j*T+tt] < s.Jobs[i].GPU || Available_CPU[j*T+tt] < s.Jobs[i].CPU {
						//	fmt.Println(i, j, t, "llll")
						found = false
						break
					}
				}
				if found {
					//fmt.Println(i, j, t, "kkkk")
					break

				}
			}
		}

		if found {
			if t == 0 {
				Targets[i] = j
			}

			for tt := t; tt < min(s.T, t+int(s.Jobs[i].RemainTime)); tt++ {
				Available_GPU[j*T+tt] -= s.Jobs[i].GPU
				Available_CPU[j*T+tt] -= s.Jobs[i].CPU

			}
		}
	}
	return Targets

}

func (s *Scheduler) LPSolve(obj []float64) ([]SchedulingDecision, float64) {

	N := s.N
	M := s.M
	T := s.T
	SchedulingDecisions := make([]SchedulingDecision, N)
	OPT := 0.0
	// Set up the problem.

	rb := []clp.Bounds{}
	mat := clp.NewPackedMatrix()
	//obj := make([]float64, N*M*T)
	//set coeff of x in each row (constraint) and in objective function
	//Index is the index of constraint, Value is the coefficient of variable
	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				tmp := []clp.Nonzero{}
				tmp = append(tmp, clp.Nonzero{Index: i, Value: 1.0})                         // placement constraint
				tmp = append(tmp, clp.Nonzero{Index: N + 2*M*T + i*M*T + j*T + t, Value: 1}) //0<=x_ij^t<=1 constraint;2 is for two capacity constarint:cpu and gpu

				for tt := t; tt < T; tt++ { // capacity constraint: cpu and gpu
					if s.Jobs[i].CPU > 0 {
						tmp = append(tmp, clp.Nonzero{Index: N + j*T + tt, Value: float64(s.Jobs[i].CPU)})

					}
					if s.Jobs[i].GPU > 0 {
						tmp = append(tmp, clp.Nonzero{Index: N + M*T + j*T + tt, Value: float64(s.Jobs[i].GPU)})
					}
					if tt > t+int(s.Jobs[i].RemainTime) {
						break
					}
				}
				mat.AppendColumn(tmp)

			}
		}
	}
	for i := 0; i < N; i++ {
		rb = append(rb, clp.Bounds{Lower: 0, Upper: 1})
	}
	for j := 0; j < M; j++ {
		for t := 0; t < T; t++ {
			rb = append(rb, clp.Bounds{Lower: 0, Upper: float64(s.Clusters[j].CPU)}) //cpu capacity limit
		}
	}
	for j := 0; j < M; j++ {
		for t := 0; t < T; t++ {
			rb = append(rb, clp.Bounds{Lower: 0, Upper: float64(s.Clusters[j].GPU)}) //gpu capacity limit
		}
	}
	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < int(s.T); t++ {
				rb = append(rb, clp.Bounds{Lower: 0, Upper: 1})

			}

		}
	}

	simp := clp.NewSimplex()
	simp.LoadProblem(mat, nil, obj, rb, nil)
	simp.SetOptimizationDirection(clp.Maximize)

	// Solve the optimization problem.
	simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	OPT = simp.ObjectiveValue()
	soln := simp.PrimalColumnSolution()
	for i := 0; i < N; i++ {
		SchedulingDecisions[i].i = i
		SchedulingDecisions[i].j = -1
		SchedulingDecisions[i].xbar = 0
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				if soln[i*M*T+j*T+t] > 0 {
					SchedulingDecisions[i].t = t
					SchedulingDecisions[i].j = j
					SchedulingDecisions[i].xbar = soln[i*M*T+j*T+t]
				}
			}
		}
	}
	return SchedulingDecisions, OPT
}
