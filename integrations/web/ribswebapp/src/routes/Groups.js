import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatNum } from "../helpers/fmt";
import "./Groups.css";
import "./Deal.css";

function Deal({ deal }) {
    const {
        UUID,
        Provider,
        PubCid,
        DealID,
        Sealed,
        Failed,
        BytesRecv,
        TxSize,
        Status,
        SealStatus,
        Error,
    } = deal;

    const errorMessage = Error.length > 49 ? `${Error.slice(0, 24)}...${Error.slice(-24)}` : Error;

    return (
        <div className={`Deal${Failed ? " deal-failed" : ""}${Sealed ? " deal-sealed" : ""}`}>
            <abbr title={UUID}>
                {UUID.substring(0, 8)}... {Sealed && <strong>SEALED</strong>}
            </abbr>
            <span>
        <a href={`https://filfox.info/en/address/f0${Provider}`} target="_blank" rel="noopener noreferrer">
          f0{Provider}
        </a>
      </span>
            {PubCid && (
                <span>
                    Deal: <a href={`https://filfox.info/en/message/${PubCid}`} target="_blank" rel="noopener noreferrer">bafy..{PubCid.substr(-16)}</a>
                    {DealID>0 && <span> <a href={`https://filfox.info/en/deal/${DealID}`} target="_blank" rel="noopener noreferrer">{DealID}</a></span>}
                </span>
            )}
            {!Sealed && (
                <>
                    {BytesRecv > 0 && <span>{formatBytesBinary(BytesRecv)} / {formatBytesBinary(TxSize)}</span>}
                    {Error === "" ? (<span>{Status} {SealStatus && ` (${SealStatus})`}</span>) : (<span>Error ({Status}) <abbr title={Error} className="deal-err">{errorMessage}</abbr></span>)}
                </>
            )}
        </div>
    );
}

const groupStateText = [
    "Writable",
    "Full",
    "BSST Exists",
    "Level Index Dropped",
    "VRCAR Done",
    "Has Commp",
    "Deals in Progress",
    "Deals Done",
    "Offloaded"
];

function Group({ group }) {
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
        <div className="group">
            <h3>
                Group {group.GroupKey}
                <span className="group-state">
          {groupStateText[group.State]}
        </span>
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
            {dealsToDisplay.length > 0 && (
                <>
                    <h4>Deals:</h4>
                    <div className="Group-deals">
                        {dealsToDisplay.map((deal) => (
                            <Deal key={deal.UUID} deal={deal} />
                        ))}
                    </div>
                </>
            )}
        </div>
    );
}


function Groups() {
    const [groups, setGroups] = useState([]);

    const fetchGroups = async () => {
        try {
            const groupKeys = await RibsRPC.call("Groups");
            const groupMetas = await Promise.all(
                groupKeys.map(async (groupKey) => {
                    let deal = await RibsRPC.call("GroupMeta", [groupKey])
                    return { ...deal, GroupKey: groupKey }
                })
            );
            setGroups(groupMetas);
        } catch (error) {
            console.error("Error fetching groups:", error);
        }
    };

    useEffect(() => {
        fetchGroups();
        const intervalId = setInterval(fetchGroups, 500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div className="Groups">
            <h2>Groups</h2>
            {groups.map((group, index) => (
                <Group key={group.GroupKey} group={group} />
            ))}
        </div>
    );
}

export default Groups;
