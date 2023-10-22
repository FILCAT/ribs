import React, {useEffect, useState} from 'react';
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatNum, epochToDate, epochToDuration } from "../helpers/fmt";
import { Group } from "./Groups";

export default function Repair() {
    const [workerStates, setWorkerStates] = useState('');
    const [queueStats, setQueueStats] = useState('');

    /*
    * 	RepairStats() (map[int]RepairJob, error)

type RepairJob struct {
	GroupKey GroupKey

	State RepairJobState

	FetchProgress, FetchSize int64
	FetchUrl                 string
}
    * */


    const fetchWorkerStates = async () => {
        try {
            const result = await RibsRPC.call("RepairStats", []);
            setWorkerStates(result);

            const queueStats = await RibsRPC.call("RepairQueue", []);
            setQueueStats(queueStats);
        } catch (error) {
            console.error("Error fetching worker states:", error);
        }
    }

    useEffect(() => {
        fetchWorkerStates();
        const intervalId = setInterval(fetchWorkerStates, 1000);

        return () => {
            clearInterval(intervalId);
        };
    });

    return (
        <div className="Content">
            <div>
                <h1>Repair</h1>
            </div>
            <div>
                <h2>Repair Queue</h2>
                <table className="compact-table">
                    <thead>
                    <tr>
                        <th>Total</th>
                        <th>Assigned</th>
                    </tr>
                    </thead>
                    <tbody>
                    <tr>
                        <td>{queueStats.Total}</td>
                        <td>{queueStats.Assigned}</td>
                    </tr>
                    </tbody>
                </table>
            </div>
            <div>
                <h2>Worker States</h2>
                <table className="compact-table">
                    <thead>
                    <tr>
                        <th>Worker</th>
                        <th>Group</th>
                        <th>State</th>
                        <th>Fetch Size</th>
                        <th>Fetch Progress</th>
                        <th>Fetch URL</th>
                    </tr>
                    </thead>
                    <tbody>
                    {workerStates && Object.keys(workerStates).map((key) => {
                        const workerState = workerStates[key];
                        return (
                            <tr key={key}>
                                <td>{key}</td>
                                <td>{workerState.GroupKey}</td>
                                <td>{workerState.State}</td>
                                <td>{formatBytesBinary(workerState.FetchSize)} / {formatBytesBinary(workerState.FetchProgress)} {formatNum(workerState.FetchProgress / (workerState.FetchSize+1) * 100, 2)}%</td>
                                <td>
                                    <div className="progress-bar thin-bar">
                                        <div className="progress-bar__fill" style={{ width: `${workerState.FetchProgress / (workerState.FetchSize+1) * 100}%` }}></div>
                                    </div>
                                </td>
                                <td>{workerState.FetchUrl}</td>
                            </tr>
                        );
                    })}
                    </tbody>
                </table>
            </div>
        </div>
    )
}
