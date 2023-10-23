import React, {useEffect, useState, useRef} from 'react';
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatNum, epochToDate, epochToDuration, calcEMA } from "../helpers/fmt";
import { Group } from "./Groups";
import {Link} from "react-router-dom";

export default function Repair() {
    const [workerStates, setWorkerStates] = useState({});
    const [queueStats, setQueueStats] = useState({});
    const prevWorkerStatesRef = useRef({});
    const prevFetchTimeRef = useRef(Date.now());
    const rateEMARef = useRef({});

    const smoothingFactor = 1 / 10;

    const fetchWorkerStates = async () => {
        try {
            const result = await RibsRPC.call("RepairStats", []);
            setWorkerStates(result);

            const qStats = await RibsRPC.call("RepairQueue", []);
            setQueueStats(qStats);
        } catch (error) {
            console.error("Error fetching worker states:", error);
        }
    };

    useEffect(() => {
        const prevWorkerStates = prevWorkerStatesRef.current;
        const currentTime = Date.now();
        const elapsedTime = (currentTime - prevFetchTimeRef.current) / 1000;

        for (const [key, workerState] of Object.entries(workerStates)) {
            const prevWorkerState = prevWorkerStates[key] || {};
            if (prevWorkerState.FetchProgress === undefined) continue;

            const fetchProgressDelta = workerState.FetchProgress - prevWorkerState.FetchProgress;
            const currentFetchRate = fetchProgressDelta / elapsedTime;

            rateEMARef.current[key] = calcEMA(
                currentFetchRate,
                rateEMARef.current[key] || 0,
                smoothingFactor
            );
        }

        prevFetchTimeRef.current = currentTime;
        prevWorkerStatesRef.current = { ...workerStates };
    }, [workerStates]);

    useEffect(() => {
        fetchWorkerStates();
        const intervalId = setInterval(fetchWorkerStates, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

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
                        <td>{queueStats.Total} Group(s)</td>
                        <td>{queueStats.Assigned} Group(s)</td>
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
                        <th>Fetch Rate</th>
                        <th>Fetch Progress</th>
                        <th>Fetch URL</th>
                    </tr>
                    </thead>
                    <tbody>
                    {Object.keys(workerStates).map((key) => {
                        const workerState = workerStates[key];
                        const fetchRate = rateEMARef.current[key] || 0;
                        return (
                            <tr key={key}>
                                <td>{key}</td>
                                <td><Link to={`/groups/${workerState.GroupKey}`}>{workerState.GroupKey}</Link></td>
                                <td>{workerState.State}</td>
                                <td>{formatBytesBinary(workerState.FetchSize)} / {formatBytesBinary(workerState.FetchProgress)} {formatNum(workerState.FetchProgress / (workerState.FetchSize+1) * 100, 2)}%</td>
                                <td>{formatBytesBinary(fetchRate)}/s</td>
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
    );
}
