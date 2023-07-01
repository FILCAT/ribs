import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatNum, epochToDate, epochToDuration } from "../helpers/fmt";
import "./Groups.css";
import "./Deal.css";
import {Link} from "react-router-dom";

function Deal({ deal, headHeight }) {
    const {
        UUID,
        Provider,
        PubCid,
        DealID,
        Sealed,
        Failed,
        StartEpoch,
        EndEpoch,
        BytesRecv,
        TxSize,
        Status,
        SealStatus,
        Error,
        RetrSuccess,
        RetrFail,
    } = deal;

    const errorMessage = Error.length > 49 ? `${Error.slice(0, 24)}...${Error.slice(-24)}` : Error;

    return (
        <div className={`Deal${Failed ? " deal-failed" : ""}${Sealed ? " deal-sealed" : ""}${RetrSuccess > 0 ? " deal-sealed-retr":""}`}>
            <span>
                <abbr title={UUID}>{UUID.substring(0, 8)}... </abbr>
                <Link to={`/provider/f0${Provider}`}>f0{Provider}</Link>
                {Sealed && <><strong> SEALED</strong> {
                    (RetrSuccess == 0 && RetrFail == 0) ? "" : (
                        RetrSuccess > 0 ? <i>RETRIEVABLE</i> : <span className="deal-error">unretrievable</span>
                    )
                }</>}
            </span>
            <span>
                {Sealed ? <> Ends {epochToDate(EndEpoch)} {epochToDuration(EndEpoch-headHeight)}</> :
                    <> Expires {epochToDuration(StartEpoch-headHeight)}</>}
            </span>
            {PubCid && (
                <span>
                    Deal: <a href={`https://filfox.info/en/message/${PubCid}`} target="_blank" rel="noopener noreferrer">bafy..{PubCid.substr(-16)}</a>
                    {DealID>0 && <span> <a href={`https://filfox.info/en/deal/${DealID}`} target="_blank" rel="noopener noreferrer">{DealID}</a></span>}
                </span>
            )}
            {!Sealed && (
                <>
                    {BytesRecv > 0 && <>
                        <span>{formatBytesBinary(BytesRecv)} / {formatBytesBinary(TxSize)}</span>
                        <div className="progress-bar">
                            <div
                                className="progress-bar__fill"
                                style={{ width: `${BytesRecv/TxSize*100}%` }}
                            ></div>
                        </div>
                    </>}
                    {Error === "" ?
                        (<span>{Status} {isSealingStatus(Status) && SealStatus && ` (${SealStatus})`}</span>) :
                        (<span>Error ({Status}) <abbr title={Error} className="deal-err">{errorMessage}</abbr></span>)}
                </>
            )}
        </div>
    );
}

function isSealingStatus(status) {
    return status != "Accepted"
}

const groupStateText = [
    "Writable",
    "Full",
    "VRCAR Done",
    "Deals in Progress",
    "Offloaded"
];

const GroupStateWritable = 0;
const GroupStateOffloaded = 4;

export function Group({ groupKey, headHeight }) {
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

    const [showFailedDeals, setShowFailedDeals] = useState(false);
    const toggleShowFailedDeals = () => setShowFailedDeals(!showFailedDeals);

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

    const dealsToDisplay = showFailedDeals
        ? group.Deals
        : group.Deals.filter((deal) => !deal.Failed);

    return (
        <div className="group" >
            <div className="group-info">
                <h3>
                    Group {group.GroupKey}
                    <span className="group-state">{groupStateText[group.State]}</span>
                </h3>
                <p>
                    Blocks: {formatNum(group.Blocks)} / {formatNum(group.MaxBlocks)}
                </p>
                {group.State === 0 && renderProgressBar(group.Blocks, group.MaxBlocks) }
                <p>
                    Bytes: {formatBytesBinary(group.Bytes)} /{" "}
                    {formatBytesBinary(group.MaxBytes)}
                </p>
                {group.State === 0 && renderProgressBar(group.Bytes, group.MaxBytes) }
                <div className="deal-counts">
                    {dealCounts.sealed > 0 && <span><span className="deal-counts-seal">{dealCounts.sealed} Sealed</span> | </span>}
                    {dealCounts.started > 0 && <span><span className="deal-counts-start">{dealCounts.started} Started</span> | </span>}
                    {dealCounts.errors > 0 && <span><span className="deal-counts-err">{dealCounts.errors} Errored</span></span>}
                </div>
                {group.Deals.length > 0 && (<button onClick={toggleShowFailedDeals}>{showFailedDeals ? "Hide" : "Show"} Failed Deals</button>)}
            </div>
                {dealsToDisplay.length > 0 && (
                    <>
                        {dealsToDisplay.map((deal) => (
                            <Deal key={deal.UUID} deal={deal} headHeight={headHeight} />
                        ))}
                    </>
                )}
        </div>
    );
}

function Groups() {
    const [groupKeys, setGroupKeys] = useState([]);
    const [headHeight, setHeadHeight] = useState(0);

    const fetchGroups = async () => {
        try {
            const groupKeys = await RibsRPC.call("Groups");
            setGroupKeys(groupKeys);

            const head = await RibsRPC.callFil("ChainHead");
            setHeadHeight(head.Height);
        } catch (error) {
            console.error("Error fetching groups:", error);
        }
    };

    useEffect(() => {
        fetchGroups();
        const intervalId = setInterval(fetchGroups, 5000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div className="Groups">
            <h2>Groups</h2>
            {groupKeys.map((groupKey, index) => (
                <Group key={groupKey} groupKey={groupKey} headHeight={headHeight} />
            ))}
        </div>
    );
}

export default Groups;
