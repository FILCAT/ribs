import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatNum, epochToDate, epochToDuration } from "../helpers/fmt";
import "./Groups.css";
import "./Deal.css";
import {Deal, GroupStateWritable, GroupStateOffloaded, groupStateText} from "./Groups";
import {useParams} from "react-router-dom";

export default function Group() {
    let { groupKey } = useParams();
    // groupkey to int
    groupKey = parseInt(groupKey)

    const [headHeight, setHeadHeight] = useState(0);
    const [group, setGroup] = useState({
        Deals: [],
    });

    let refreshing = false;

    const fetchGroup = async () => {
        try {
            if (refreshing) return;
            refreshing = true;

            let now = Date.now();
            let sinceLastRefresh = now - group.lastRefresh;

            if (group.State === GroupStateWritable && sinceLastRefresh < 500) {
                // writable groups at most every 500ms
                return;
            } else if (group.State === GroupStateOffloaded && sinceLastRefresh < 120000) {
                // offloaded groups at most every 120s
                return;
            } else if (group.State !== GroupStateWritable && sinceLastRefresh < 10000) {
                // non-writable groups at most every 10s
                return;
            }

            console.log("fetching group", groupKey, sinceLastRefresh, refreshing)

            let meta = await RibsRPC.call("GroupMeta", [groupKey]);
            let deals = await RibsRPC.call("GroupDeals", [groupKey]);

            now = Date.now();

            refreshing = false;
            let groupData = { ...meta, GroupKey: groupKey, Deals: deals, lastRefresh: now };

            setGroup(groupData);

            const head = await RibsRPC.callFil("ChainHead");
            setHeadHeight(head.Height);
        } catch (error) {
            console.error("Error fetching group:", error);
        }
    };

    useEffect(() => {
        fetchGroup();
        const intervalId = setInterval(fetchGroup, 500);

        return () => {
            clearInterval(intervalId);
        };
    }, [groupKey]);

    const renderProgressBar = (bytes, maxBytes) => {
        const percentage = (bytes / maxBytes) * 100;
        return (
            <div className="progress-bar">
                <div
                    className="progress-bar__fill"
                    style={{ width: `${percentage}%` }}
                ></div>
            </div>
        );
    };

    const dealCounts = group.Deals.reduce(
        (counts, deal) => {
            if (deal.Failed) counts.errors++;
            else if (deal.Sealed) counts.sealed++;
            else counts.started++;

            return counts;
        },
        { started: 0, sealed: 0, errors: 0 }
    );

    const dealsToDisplay = group.Deals

    return (
        <div>
            <div>
                <h3>
                    Group {group.GroupKey}
                </h3>
                <span>State: <b>{groupStateText[group.State]}</b></span>
                <p>
                    Blocks: {formatNum(group.Blocks)} / {formatNum(group.MaxBlocks)}
                </p>
                {group.State === GroupStateWritable && renderProgressBar(group.Blocks, group.MaxBlocks) }
                <p>
                    Bytes: {formatBytesBinary(group.Bytes)} /{" "}
                    {formatBytesBinary(group.MaxBytes)}
                </p>
                {group.State === GroupStateWritable && renderProgressBar(group.Bytes, group.MaxBytes) }
                <div className="deal-counts">
                    {dealCounts.sealed > 0 && <span><span className="deal-counts-seal">{dealCounts.sealed} Sealed</span> | </span>}
                    {dealCounts.started > 0 && <span><span className="deal-counts-start">{dealCounts.started} Started</span> | </span>}
                    {dealCounts.errors > 0 && <span><span className="deal-counts-err">{dealCounts.errors} Errored</span></span>}
                </div>
                <div>PieceCID: {group.PieceCID} <a target="_blank" href={`https://filecoin.tools/${group.PieceCID}`}>[filecoin.tools]</a></div>
                <div>RootCID: {group.RootCID}</div>
            </div>
            <div className="group" >
                    {dealsToDisplay.length > 0 && (
                        <>
                            {dealsToDisplay.map((deal) => (
                                <Deal key={deal.UUID} deal={deal} headHeight={headHeight} pieceCid={group.PieceCID} dataCid={group.RootCID} />
                            ))}
                        </>
                    )}
            </div>
        </div>
    );
}