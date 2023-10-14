package rbdeal

/*

REPAIR WORKERS:
* check if they have active work
* if not, manage repair queue
* go to top

If they have active work:
* Fetch sector
	* Http if possible
	* lassie if not..
* Verify and send to storage, move group to deals in progress

Tables:
* repair_queue: group, retrievable_deals, assigned_worker, last_attempt

*/

func (r *ribs) repairWorker() { // root, id?
	for {
		select {
		case <-r.close:
			return
		default:
		}

		// check assigned work

		if false {
			// or do work assigning

			continue
		}

		// do work
		//assignedGroup := ribs2.GroupKey(0)

		// fetch sector

	}
}

func (r *ribs) fetchSector() {

}

func (r *ribs) fetchSectorHttp() {

}

func (r *ribs) fetchSectorLassie() {

}
