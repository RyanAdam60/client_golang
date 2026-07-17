package prometheus

import (
	"sync"
)

type Registry struct {
	mtx           sync.RWMutex
	collectors    map[Collector]struct{}
	collecting    map[Collector]*sync.WaitGroup
}

func (r *Registry) Unregister(c Collector) {
	r.mtx.Lock()
	delete(r.collectors, c)
	wg, ok := r.collecting[c]
	r.mtx.Unlock()

	if ok {
		wg.Wait()
	}
}

func (r *Registry) Gather() ([]MetricFamily, error) {
	r.mtx.RLock()
	collectors := make([]Collector, 0, len(r.collectors))
	for c := range r.collectors {
		collectors = append(collectors, c)
	}
	r.mtx.RUnlock()

	var wg sync.WaitGroup
	for _, c := range collectors {
		r.mtx.Lock()
		if r.collecting[c] == nil {
			r.collecting[c] = &sync.WaitGroup{}
		}
		r.collecting[c].Add(1)
		wg.Add(1)
		go func(col Collector) {
			defer wg.Done()
			defer func() {
				r.mtx.Lock()
				r.collecting[col].Done()
				r.mtx.Unlock()
			}()
			col.Collect(nil) // Simplified for example
		}(c)
		r.mtx.Unlock()
	}
	wg.Wait()
	return nil, nil
}