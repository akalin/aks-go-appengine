package aksprimalitytester

import "appengine"
import "appengine/datastore"
import "appengine/taskqueue"
import "encoding/json"
import "fmt"
import "github.com/akalin/aks-go/aks"
import "html/template"
import "log"
import "math/big"
import "net/http"
import "runtime"
import "net/url"
import "os"

type Job struct {
	N            string
	R            string
	M            string
	NonWitnesses []string
	Witnesses    []string
}

func init() {
	http.HandleFunc("/", rootHandler)
	http.HandleFunc("/uploadJob", uploadJobHandler)
	http.HandleFunc("/startJob", startJobHandler)
	http.HandleFunc("/processJob", processJobHandler)
	http.HandleFunc("/getJobs", getJobsHandler)
	http.HandleFunc("/getAKSWitness", getAKSWitnessHandler)
}

func emitError(c appengine.Context, w http.ResponseWriter, error string) {
	c.Errorf("%s", error)
	http.Error(w, error, http.StatusInternalServerError)
}

var rootTemplate = template.Must(template.ParseFiles("templates/root.html"))

func rootHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	if err := rootTemplate.Execute(w, nil); err != nil {
		emitError(c, w, err.Error())
		return
	}
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
		N: n.String(),
		R: R.String(),
		M: M.String(),
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
	var M big.Int
	M.SetString(job.M, 10)
	for i := big.NewInt(1); i.Cmp(&M) < 0; i.Add(i, big.NewInt(1)) {
		b, err := json.Marshal(i.String())
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

	var jobTasks []*taskqueue.Task
	numJobTasks := 4
	for i := 0; i < numJobTasks; i++ {
		jobTask := taskqueue.NewPOSTTask(
			"/processJob", url.Values{"key": {key.Encode()}})
		host := appengine.BackendHostname(c, "job-processor", i)
		jobTask.Header.Set("Host", host)
		jobTasks = append(jobTasks, jobTask)
	}
	if _, err := taskqueue.AddMulti(c, jobTasks, "job-queue"); err != nil {
		emitError(c, w, err.Error())
		return
	}

	fmt.Fprintf(w, "Processing %s", key.Encode())
}

func processPotentialWitnessTask(
	n *big.Int, r *big.Int, task *taskqueue.Task, maxOutstanding int,
	w http.ResponseWriter, logger *log.Logger) (string, bool, error) {
	var potentialWitnessStr string
	if err := json.Unmarshal(
		task.Payload, &potentialWitnessStr); err != nil {
		return "", false, err
	}

	var potentialWitness big.Int
	potentialWitness.SetString(potentialWitnessStr, 10)

	start := potentialWitness
	var end big.Int
	end.Add(&start, big.NewInt(1))
	a := aks.GetAKSWitness(n, r, &start, &end, maxOutstanding, logger)

	return potentialWitnessStr, a != nil, nil
}

func processPotentialWitnessTasks(
	c appengine.Context,
	n *big.Int, r *big.Int, tasks []*taskqueue.Task, maxOutstanding int,
	w http.ResponseWriter,
	logger *log.Logger) ([]string, []string, error) {

	var newWitnesses []string
	var newNonWitnesses []string
	for _, task := range tasks {
		potentialWitness, isWitness, err :=
			processPotentialWitnessTask(
				n, r, task, maxOutstanding, w, logger)
		if err != nil {
			return nil, nil, err
		}

		if isWitness {
			newWitnesses =
				append(newWitnesses, potentialWitness)
			c.Infof("%s is an AKS witness for %v",
				potentialWitness, n)
			break
		} else {
			newNonWitnesses =
				append(newNonWitnesses, potentialWitness)
			c.Infof("%s is not an AKS witness for %v",
				potentialWitness, n)
		}
	}
	return newWitnesses, newNonWitnesses, nil
}

func appendResultsToJob(c appengine.Context, key *datastore.Key,
	newWitnesses []string, newNonWitnesses []string) (Job, error) {
	var job Job
	if err := datastore.Get(c, key, &job); err != nil {
		return Job{}, err
	}
	for _, w := range newWitnesses {
		job.Witnesses = append(job.Witnesses, w)
	}
	for _, w := range newNonWitnesses {
		job.NonWitnesses = append(job.NonWitnesses, w)
	}
	if _, err := datastore.Put(c, key, &job); err != nil {
		return Job{}, err
	}
	return job, nil
}

func processJobHandler(w http.ResponseWriter, r *http.Request) {
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
	var n big.Int
	n.SetString(job.N, 10)
	var R big.Int
	R.SetString(job.R, 10)

	logger := log.New(os.Stderr, "", 0)
	numCPU := runtime.NumCPU()
	// TODO(akalin): Figure out a better way to limit this.
	secPerPotentialWitness := 1000
	for {
		tasks, err := taskqueue.LeaseByTag(
			c, numCPU, "potential-witness-queue",
			numCPU*secPerPotentialWitness, key.Encode())
		if err != nil {
			emitError(c, w, err.Error())
			return
		}
		if len(tasks) == 0 {
			break
		}

		if len(job.Witnesses) == 0 {
			newWitnesses, newNonWitnesses, err :=
				processPotentialWitnessTasks(
					c, &n, &R, tasks, numCPU, w, logger)

			if err != nil {
				emitError(c, w, err.Error())
				return
			}

			appendToJob := func(c appengine.Context) error {
				job, err = appendResultsToJob(
					c, key, newWitnesses, newNonWitnesses)
				return err
			}

			if err := datastore.RunInTransaction(
				c, appendToJob, nil); err != nil {
				emitError(c, w, err.Error())
				return
			}
		}

		err = taskqueue.DeleteMulti(
			c, tasks, "potential-witness-queue")
		if err != nil {
			emitError(c, w, err.Error())
			return
		}
	}

	fmt.Fprintf(w, "Processed %s", key.Encode())
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
