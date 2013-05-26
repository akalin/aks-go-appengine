package aksprimalitytester

import "appengine"
import "appengine/datastore"
import "appengine/taskqueue"
import "encoding/json"
import "fmt"
import "github.com/akalin/aks-go/aks"
import "log"
import "math/big"
import "net/http"
import "runtime"

type Job struct {
	// TODO(akalin): Use (serialized) big.Ints instead.
	N            int64
	R            int64
	M            int64
	NonWitnesses []int64
	Witnesses    []int64
}

func init() {
	http.HandleFunc("/uploadJob", uploadJobHandler)
	http.HandleFunc("/startJob", startJobHandler)
	http.HandleFunc("/getJobs", getJobsHandler)
	http.HandleFunc("/getAKSWitness", getAKSWitnessHandler)
}

func emitError(c appengine.Context, w http.ResponseWriter, error string) {
	c.Errorf("%s", error)
	http.Error(w, error, http.StatusInternalServerError)
}

func parseFormBigInt(
	c appengine.Context, w http.ResponseWriter,
	r *http.Request, key string) *big.Int {
	str := r.FormValue(key)
	var i big.Int
	if _, parsed := i.SetString(str, 10); !parsed {
		emitError(c, w, "Could not parse "+key+"="+str)
		return nil
	}
	return &i
}

func uploadJobHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	// TODO(akalin): Error out if the request isn't a POST.

	n := parseFormBigInt(c, w, r, "n")

	if n == nil {
		return
	}

	// TODO(akalin): Filter out n < 2.

	R := aks.CalculateAKSModulus(n)
	M := aks.CalculateAKSUpperBound(n, R)

	// TODO(akalin): Check for factors < M and then whether M^2 >
	// N.

	job := Job{
		N: n.Int64(),
		R: R.Int64(),
		M: M.Int64(),
	}
	incompleteKey := datastore.NewIncompleteKey(c, "Job", nil)
	key, err := datastore.Put(c, incompleteKey, &job)
	if err != nil {
		emitError(c, w, err.Error())
		return
	}

	fmt.Fprintf(w, "%s", key.Encode())
}

func startJobHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	// TODO(akalin): Error out if the request isn't a POST.

	keyStr := r.FormValue("key")
	key, err := datastore.DecodeKey(keyStr)
	if err != nil {
		emitError(c, w, err.Error())
		return
	}
	var job Job
	if err = datastore.Get(c, key, &job); err != nil {
		emitError(c, w, err.Error())
		return
	}

	var tasks []*taskqueue.Task
	for i := int64(1); i < job.M; i++ {
		b, err := json.Marshal(i)
		if err != nil {
			emitError(c, w, err.Error())
			return
		}
		task := &taskqueue.Task{
			Method:  "PULL",
			Tag:     keyStr,
			Payload: b,
		}
		tasks = append(tasks, task)
	}

	batchSize := 100
	for i := 0; i < len(tasks); i += batchSize {
		end := i + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		_, err := taskqueue.AddMulti(
			c, tasks[i:end], "potential-witness-queue")
		if err != nil {
			emitError(c, w, err.Error())
			return
		}
	}

	// TODO(akalin): Kick off tasks to process the tasks just
	// added to potentialWitnessQueue.

	fmt.Fprintf(w, "Processing %s", key.Encode())
}

func getJobsHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	q := datastore.NewQuery("Job")

	var jobs []*Job
	if _, err := q.GetAll(c, &jobs); err != nil {
		emitError(c, w, err.Error())
		return
	}

	if err := json.NewEncoder(w).Encode(jobs); err != nil {
		emitError(c, w, err.Error())
		return
	}
}

func getAKSWitnessHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	n := parseFormBigInt(c, w, r, "n")
	if n == nil {
		return
	}

	R := parseFormBigInt(c, w, r, "r")
	if R == nil {
		return
	}

	start := parseFormBigInt(c, w, r, "start")
	if start == nil {
		return
	}

	end := parseFormBigInt(c, w, r, "end")
	if end == nil {
		return
	}

	jobs := runtime.NumCPU()

	logger := log.New(w, "", 0)
	a := aks.GetAKSWitness(n, R, start, end, jobs, logger)
	if a != nil {
		fmt.Fprintf(w, "%v has AKS witness %v\n", n, a)
	} else {
		fmt.Fprintf(
			w, "%v has no AKS witness in [%v, %v)\n",
			n, start, end)
	}
}
