import './Status.css';
import React, { useState, useEffect, useRef } from "react";
import RibsRPC from "../helpers/rpc";
import { CopyToClipboard } from 'react-copy-to-clipboard';
import {formatBytesBinary, formatBitsBinary, formatNum, formatNum6, calcEMA} from "../helpers/fmt";

const oneFil = 1000000000000000000

function WalletInfoTile({ walletInfo }) {
    const [dropdownOpen, setDropdownOpen] = useState(false);
    const [amount, setAmount] = useState(oneFil);
    const [operationType, setOperationType] = useState('');

    const truncateAddress = (address) => {
        const head = address.slice(0, 10);
        const tail = address.slice(-10);
        return `${head}...${tail}`;
    };

    const handleAddWithdrawClick = (type) => {
        setOperationType(type);
        setDropdownOpen(!dropdownOpen);
    };

    const handleAmountChange = (event) => {
        setAmount(event.target.value);
    };

    const handleSubmit = async () => {
        try {
            let cid;
            if (operationType === 'add') {
                cid = await RibsRPC.call("WalletMarketAdd", [(amount*oneFil).toString()]);
            } else {
                cid = await RibsRPC.call("WalletMarketWithdraw", [(amount*oneFil).toString()]);
            }
            alert(`Operation successful. CID: ${cid['/']}`);
            setDropdownOpen(false);
        } catch (error) {
            console.error("Error during operation:", error);
            alert('Error during operation. Please try again.');
        }
    };

    return (
        <div>
            <h2>Wallet Info</h2>
            {walletInfo && (
                <table className="compact-table">
                    <tbody>
                    <tr>
                        <td>Address</td>
                        <td>{truncateAddress(walletInfo.Addr)}</td>
                    </tr>
                    <tr>
                        <td>Balance:</td>
                        <td>{walletInfo.Balance} [Send]</td>
                    </tr>
                    <tr>
                        <td>Market Balance:</td>
                        <td>
                            {walletInfo.MarketBalance}
                            {' '}
                            <button className="button-sm" onClick={() => handleAddWithdrawClick('add')}>Add</button>
                            {' '}
                            <button className="button-sm" onClick={() => handleAddWithdrawClick('withdraw')}>Withdraw</button>
                        </td>
                    </tr>
                    {dropdownOpen && (
                        <tr>
                            <td colSpan="2">
                                <input
                                    type="text"
                                    value={amount}
                                    onChange={handleAmountChange}
                                    placeholder="Enter amount"
                                />
                                <button className="button-sm" onClick={handleSubmit}>{operationType === 'add' ? 'Add' : 'Withdraw'}</button>
                            </td>
                        </tr>
                    )}
                    <tr>
                        <td>Market Locked:</td>
                        <td>{walletInfo.MarketLocked}</td>
                    </tr>
                    <tr>
                        <td>DataCap:</td>
                        <td className="important-metric">{walletInfo.DataCap}</td>
                    </tr>
                    </tbody>
                </table>
            )}
        </div>
    );
}

function GroupsTile() {
    const [groupStats, setGroupStats] = useState(null);

    const fetchStatus = async () => {
        try {
            const groupStats = await RibsRPC.call("GetGroupStats");
            setGroupStats(groupStats);
        } catch (error) {
            console.error("Error fetching group stats:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>Data Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Block Groups:</td>
                    <td>{groupStats?.GroupCount}</td>
                </tr>
                <tr>
                    <td>Total data size:</td>
                    <td className="important-metric">{formatBytesBinary(groupStats?.TotalDataSize)}</td>
                </tr>
                <tr>
                    <td>Local size:</td>
                    <td>{formatBytesBinary(groupStats?.NonOffloadedDataSize)}</td>
                </tr>
                <tr>
                    <td>Offloaded size:</td>
                    <td>{formatBytesBinary(groupStats?.OffloadedDataSize)}</td>
                </tr>
                <tr>
                    <td>Open (RO):</td>
                    <td>{groupStats?.OpenGroups ?? 0}</td>
                </tr>
                <tr>
                    <td>Open (RW):</td>
                    <td>{groupStats?.OpenWritable ?? 0}</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function TopIndexTile() {
    const [indexStats, setTopIndexStats] = useState({});
    const prevStatsRef = useRef({});
    const readRateEMARef = useRef(0);
    const writeRateEMARef = useRef(0);
    const smoothingFactor = 1 / 10;

    const fetchStatus = async () => {
        try {
            const topIndexStats = await RibsRPC.call("TopIndexStats");

            const prevStats = prevStatsRef.current;
            const reads = topIndexStats.Reads;
            const writes = topIndexStats.Writes;

            if (prevStats.Reads !== undefined && prevStats.Writes !== undefined) {
                const readRate = reads - prevStats.Reads;
                const writeRate = writes - prevStats.Writes;

                readRateEMARef.current = calcEMA(
                    readRate,
                    readRateEMARef.current,
                    smoothingFactor
                );
                writeRateEMARef.current = calcEMA(
                    writeRate,
                    writeRateEMARef.current,
                    smoothingFactor
                );
            }

            setTopIndexStats({
                ...topIndexStats,
                ReadRate: Math.round(readRateEMARef.current),
                WriteRate: Math.round(writeRateEMARef.current),
            });

            prevStatsRef.current = { Reads: reads, Writes: writes };
        } catch (error) {
            console.error("Error fetching status:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>Top Index</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Entries:</td>
                    <td className="important-metric">{formatNum6(indexStats.Entries)}</td>
                </tr>
                <tr>
                    <td>Read rate:</td>
                    <td>{indexStats.ReadRate}/s</td>
                </tr>
                <tr>
                    <td>Write rate:</td>
                    <td>{indexStats.WriteRate}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}


function DealsTile({ dealSummary }) {
    return (
        <div>
            <h2>Deals: {dealSummary.InProgress + dealSummary.Done}</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Total data size:</td>
                    <td>{formatBytesBinary(dealSummary.TotalDataSize)}</td>
                </tr>
                <tr>
                    <td>Total deal size:</td>
                    <td>{formatBytesBinary(dealSummary.TotalDealSize)}</td>
                </tr>
                <tr>
                    <td>Stored data size:</td>
                    <td className="important-metric">{formatBytesBinary(dealSummary.StoredDataSize)}</td>
                </tr>
                <tr>
                    <td>Stored deal size:</td>
                    <td className="important-metric">{formatBytesBinary(dealSummary.StoredDealSize)}</td>
                </tr>
                <tr>
                    <td>Deals in progress:</td>
                    <td>{dealSummary.InProgress}</td>
                </tr>
                <tr>
                    <td>Deals done:</td>
                    <td className="important-metric">{dealSummary.Done}</td>
                </tr>
                <tr>
                    <td>Deals failed:</td>
                    <td>{dealSummary.Failed}</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function ProvidersTile({ reachableProviders }) {
    return (
        <div>
            <h2>Providers</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Reachable Providers:</td>
                    <td>{reachableProviders.length}</td>
                </tr>
                <tr>
                    <td>With booster-bitswap:</td>
                    <td>{reachableProviders.filter(p => p.BoosterBitswap).length}</td>
                </tr>
                <tr>
                    <td>With booster-http:</td>
                    <td>{reachableProviders.filter(p => p.BoosterHttp).length}</td>
                </tr>
                <tr>
                    <td>With attempted deals:</td>
                    <td>{reachableProviders.filter(p => p.DealStarted).length}</td>
                </tr>
                <tr>
                    <td>With successful deals:</td>
                    <td>{reachableProviders.filter(p => p.DealSuccess).length}</td>
                </tr>
                <tr>
                    <td>With all rejected deals:</td>
                    <td>{reachableProviders.filter(p => p.DealRejected).length}</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function CarUploadStatsTile({ carUploadStats }) {
    const [displayStats, setDisplayStats] = useState({});
    const [globalRate, setGlobalRate] = useState(0);
    const prevStatsRef = useRef({});
    const [lastGlobalBytes, setLastGlobalBytes] = useState(0);
    const rateEMARef = useRef({});
    const smoothingFactor = 1 / 10;

    const calcRates = () => {
        const newDisplayStats = {};

        let byGroup = {};
        if (carUploadStats.ByGroup) {
            byGroup = carUploadStats.ByGroup;
        }

        for (const [groupKey, uploadStats] of Object.entries(byGroup)) {
            if (!prevStatsRef.current[groupKey]) {
                // If previous stats for this group are not initialized, set them to the current stats
                prevStatsRef.current[groupKey] = uploadStats;
                continue;
            }

            const prevStats = prevStatsRef.current[groupKey] || { UploadBytes: 0 };
            const bytesSent = uploadStats.UploadBytes;
            const bytesRate = bytesSent - prevStats.UploadBytes;

            rateEMARef.current[groupKey] = calcEMA(
                bytesRate,
                rateEMARef.current[groupKey] || 0,
                smoothingFactor
            );

            newDisplayStats[groupKey] = {
                ...uploadStats,
                UploadRate: Math.round(rateEMARef.current[groupKey]),
            };

            prevStatsRef.current[groupKey] = { UploadBytes: bytesSent };
        }

        let lastBytes = lastGlobalBytes;
        if(lastBytes === 0) {
            lastBytes = carUploadStats.LastTotalBytes;
        }

        const globalBytesChange = carUploadStats.LastTotalBytes - lastBytes;
        const globalRateEMA = calcEMA(
            globalBytesChange,
            globalRate || 0,
            smoothingFactor / 10
        );

        setGlobalRate(Math.round(globalRateEMA));
        setLastGlobalBytes(carUploadStats.LastTotalBytes);
        setDisplayStats(newDisplayStats);
    };

    useEffect(() => {
        calcRates();
        const interval = setInterval(calcRates, 1000);
        return () => clearInterval(interval);
    }, [carUploadStats]);

    return (
        <div>
            <h2>Car Upload Stats</h2>
            <table className="compact-table">
                <thead>
                <tr>
                    <th style={{ width: '30%' }}>Group</th>
                    <th style={{ width: '30%' }}>Reqs</th>
                    <th style={{ width: '40%' }}>Rate</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                    <td>Global</td>
                    <td></td>
                    <td>{formatBitsBinary(globalRate)}</td>
                </tr>
                {Object.entries(displayStats).map(([groupKey, uploadStats]) => (
                    <tr key={groupKey}>
                        <td>Group {groupKey}</td>
                        <td>{uploadStats.ActiveRequests}</td>
                        <td>{formatBitsBinary(uploadStats.UploadRate)}</td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}

function CrawlStateTile({ crawlState }) {
    const progressBarPercentage = crawlState.State === "querying providers" ? (crawlState.At / crawlState.Total) * 100 : 0;
    const showAt = ["listing market participants", "querying providers"].includes(crawlState.State);

    return (
        <div className="CrawlStateTile">
            <h2>Crawl State</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td><b>State:</b></td>
                    <td>{crawlState.State}</td>
                </tr>
                {showAt && (
                    <tr>
                        <td><b>At:</b></td>
                        <td>{crawlState.At}</td>
                    </tr>
                )}
                {crawlState.State === "querying providers" && (
                    <>
                        <tr>
                            <td><b>Progress:</b></td>
                            <td>{progressBarPercentage.toFixed(2)}%</td>
                        </tr>
                        <tr>
                            <td colSpan={2}>
                                <div className="progress-bar">
                                    <div className="progress-bar__fill" style={{ width: `${progressBarPercentage}%` }}></div>
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td><b>Total:</b></td>
                            <td>{crawlState.Total}</td>
                        </tr>
                        <tr>
                            <td><b>Reachable:</b></td>
                            <td>{crawlState.Reachable}</td>
                        </tr>
                        <tr>
                            <td><b>Boost:</b></td>
                            <td>{crawlState.Boost}</td>
                        </tr>
                        <tr>
                            <td><b>BBswap:</b></td>
                            <td>{crawlState.BBswap}</td>
                        </tr>
                        <tr>
                            <td><b>BHttp:</b></td>
                            <td>{crawlState.BHttp}</td>
                        </tr>
                    </>
                )}
                </tbody>
            </table>
        </div>
    );
}

function IoStats() {
    const [groupIOStats, setGroupIOStats] = useState({});
    const prevStatsRef = useRef({});
    const readBlocksEMARef = useRef(0);
    const writeBlocksEMARef = useRef(0);
    const readBytesEMARef = useRef(0);
    const writeBytesEMARef = useRef(0);
    const smoothingFactor = 1 / 10;

    const fetchStatus = async () => {
        try {
            const ioStats = await RibsRPC.call("GroupIOStats");

            const prevStats = prevStatsRef.current;
            const readBlocks = ioStats.ReadBlocks;
            const writeBlocks = ioStats.WriteBlocks;
            const readBytes = ioStats.ReadBytes;
            const writeBytes = ioStats.WriteBytes;

            if (prevStats.ReadBlocks !== undefined && prevStats.WriteBlocks !== undefined) {
                const readBlocksRate = readBlocks - prevStats.ReadBlocks;
                const writeBlocksRate = writeBlocks - prevStats.WriteBlocks;
                const readBytesRate = readBytes - prevStats.ReadBytes;
                const writeBytesRate = writeBytes - prevStats.WriteBytes;

                readBlocksEMARef.current = calcEMA(
                    readBlocksRate,
                    readBlocksEMARef.current,
                    smoothingFactor
                );
                writeBlocksEMARef.current = calcEMA(
                    writeBlocksRate,
                    writeBlocksEMARef.current,
                    smoothingFactor
                );
                readBytesEMARef.current = calcEMA(
                    readBytesRate,
                    readBytesEMARef.current,
                    smoothingFactor
                );
                writeBytesEMARef.current = calcEMA(
                    writeBytesRate,
                    writeBytesEMARef.current,
                    smoothingFactor
                );
            }

            setGroupIOStats({
                ...ioStats,
                ReadBlocksRate: Math.round(readBlocksEMARef.current),
                WriteBlocksRate: Math.round(writeBlocksEMARef.current),
                ReadBytesRate: Math.round(readBytesEMARef.current),
                WriteBytesRate: Math.round(writeBytesEMARef.current),
            });

            prevStatsRef.current = { ReadBlocks: readBlocks, WriteBlocks: writeBlocks, ReadBytes: readBytes, WriteBytes: writeBytes };
        } catch (error) {
            console.error("Error fetching status:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>IO Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Read Rate:</td>
                    <td>{formatNum(groupIOStats.ReadBlocksRate)} Blk/s</td>
                </tr>
                <tr>
                    <td>Read Bytes:</td>
                    <td>{formatBytesBinary(groupIOStats.ReadBytesRate)}/s</td>
                </tr>
                <tr>
                    <td>Write Rate:</td>
                    <td>{formatNum(groupIOStats.WriteBlocksRate)} Blk/s</td>
                </tr>
                <tr>
                    <td>Write Bytes:</td>
                    <td>{formatBytesBinary(groupIOStats.WriteBytesRate)}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function RetrStats({retrStats}) {
    return (
        <div>
            <h2>Retrieval Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Success:</td>
                    <td>{formatNum(retrStats.Success)}</td>
                </tr>
                <tr>
                    <td>Bytes:</td>
                    <td>{formatBytesBinary(retrStats.Bytes)}</td>
                </tr>
                <tr>
                    <td>Fail:</td>
                    <td>{formatNum(retrStats.Fail)}</td>
                </tr>
                <tr>
                    <td>Cache Hit:</td>
                    <td>{formatNum(retrStats.CacheHit)}</td>
                </tr>
                <tr>
                    <td>Cache Miss:</td>
                    <td>{formatNum(retrStats.CacheMiss)}</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function StagingStats({stagingStats}) {
    const prevStatsRef = useRef({});
    const readReqsEMARef = useRef(0);
    const readBytesEMARef = useRef(0);
    const uploadBytesEMARef = useRef(0);
    const redirectsEMARef = useRef(0);
    const smoothingFactor = 1 / 10;

    useEffect(() => {
        const prevStats = prevStatsRef.current;
        const readReqs = stagingStats.ReadReqs;
        const readBytes = stagingStats.ReadBytes;
        const uploadBytes = stagingStats.UploadBytes;
        const redirects = stagingStats.Redirects;

        if (prevStats.ReadReqs !== undefined) {
            const readReqsRate = readReqs - prevStats.ReadReqs;
            const readBytesRate = readBytes - prevStats.ReadBytes;
            const uploadBytesRate = uploadBytes - prevStats.UploadBytes;
            const redirectsRate = redirects - prevStats.Redirects;

            readReqsEMARef.current = calcEMA(
                readReqsRate,
                readReqsEMARef.current,
                smoothingFactor
            );
            readBytesEMARef.current = calcEMA(
                readBytesRate,
                readBytesEMARef.current,
                smoothingFactor
            );
            uploadBytesEMARef.current = calcEMA(
                uploadBytesRate,
                uploadBytesEMARef.current,
                smoothingFactor
            );
            redirectsEMARef.current = calcEMA(
                redirectsRate,
                redirectsEMARef.current,
                smoothingFactor
            );
        }

        prevStatsRef.current = { ReadReqs: readReqs, ReadBytes: readBytes, UploadBytes: uploadBytes, Redirects: redirects };
    }, [stagingStats]);

    return (
        <div>
            <h2>Remote Staging Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Read Reqs/s:</td>
                    <td>{formatNum(Math.round(readReqsEMARef.current))}/s</td>
                </tr>
                <tr>
                    <td>Read Bytes/s:</td>
                    <td>{formatBytesBinary(Math.round(readBytesEMARef.current))}/s</td>
                </tr>
                <tr>
                    <td>Read Bytes:</td>
                    <td>{formatBytesBinary(stagingStats.ReadBytes)}</td>
                </tr>
                <tr>
                    <td>Upload Rate:</td>
                    <td>{formatBytesBinary(Math.round(uploadBytesEMARef.current))}/s</td>
                </tr>
                <tr>
                    <td>Uploaded Bytes:</td>
                    <td>{formatBytesBinary(stagingStats.UploadBytes)}</td>
                </tr>
                <tr>
                    <td>Active Uploads:</td>
                    <td>{stagingStats.UploadStarted - stagingStats.UploadDone - stagingStats.UploadErr}</td>
                </tr>
                {stagingStats.UploadErr > 0 && <tr>
                    <td>Upload Err:</td>
                    <td>{formatNum(stagingStats.UploadErr)}</td>
                </tr>}
                <tr>
                    <td>Redirects Rate:</td>
                    <td>{formatNum(Math.round(redirectsEMARef.current))}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function P2PNodes() {
    const [nodes, setNodes] = useState({});

    const fetchStatus = async () => {
        try {
            const nodeStats = await RibsRPC.call("P2PNodes");
            setNodes(nodeStats)
        } catch (error) {
            console.error("Error fetching p2p node infos:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>LibP2P Nodes</h2>
            <table className="compact-table">
                <tbody>
                {Object.keys(nodes).map((nodeName, index) => (
                    <tr key={index}>
                        <td colSpan={2}><h3>{nodeName}</h3></td>
                        <td colSpan={2}>
                            <CopyToClipboard text={nodes[nodeName].PeerID}>
                                <p title={`PeerID: ${nodes[nodeName].PeerID}\n\nListen Addresses: ${nodes[nodeName].Listen.join('\n')}`}>
                                    {`${nodes[nodeName].PeerID.slice(0, 10)}...`}
                                </p>
                            </CopyToClipboard>
                        </td>
                        <td colSpan={2}>Peers: {nodes[nodeName].Peers}</td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}

function GoRuntimeStats() {
    const [stats, setStats] = useState({});
    const prevStatsRef = useRef({});
    const prevTimeRef = useRef(Date.now());
    const gcPausePercentEMARef = useRef(0);
    const smoothingFactor = 1 / 10;

    const fetchStats = async () => {
        try {
            const runtimeStats = await RibsRPC.call("RuntimeStats");
            const currentTime = Date.now();
            const elapsedTime = currentTime - prevTimeRef.current;
            prevTimeRef.current = currentTime;

            if (prevStatsRef.current.PauseTotalNs !== undefined) {
                const gcPauseTimeDelta = runtimeStats.PauseTotalNs - prevStatsRef.current.PauseTotalNs;
                const gcPauseTimePercent = gcPauseTimeDelta / (elapsedTime * 1e6); // convert ms to ns
                gcPausePercentEMARef.current = calcEMA(gcPauseTimePercent, gcPausePercentEMARef.current, smoothingFactor);
            }

            prevStatsRef.current = runtimeStats;
            setStats(runtimeStats);
        } catch (error) {
            console.error("Error fetching Go runtime stats:", error);
        }
    };

    useEffect(() => {
        fetchStats();
        const intervalId = setInterval(fetchStats, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div>
            <h2>Go Runtime Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Alloc:</td>
                    <td>{formatBytesBinary(stats.Alloc)}</td>
                </tr>
                <tr>
                    <td>TotalAlloc:</td>
                    <td>{formatBytesBinary(stats.TotalAlloc)}</td>
                </tr>
                <tr>
                    <td>HeapAlloc:</td>
                    <td>{formatBytesBinary(stats.HeapAlloc)}</td>
                </tr>
                <tr>
                    <td>HeapSys:</td>
                    <td>{formatBytesBinary(stats.HeapSys)}</td>
                </tr>
                <tr>
                    <td>HeapObjects:</td>
                    <td>{formatNum(stats.HeapObjects)}</td>
                </tr>
                <tr>
                    <td>Number of GC:</td>
                    <td>{formatNum(stats.NumGC)}</td>
                </tr>
                <tr>
                    <td>GC Pause (% of time):</td>
                    <td>{formatNum(gcPausePercentEMARef.current * 100, 3)}%</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function RetrCheckerStats({stats}) {
    return (
        <div>
            <h2>Retrieval Checker</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Progress:</td>
                    <td>{stats.Success+stats.Fail} / {stats.ToDo} ({(stats.Success+stats.Fail) / stats.ToDo * 100}%)</td>
                </tr>
                <tr>
                    <td colSpan={2}>
                        <div className="progress-bar">
                            <div className="progress-bar__fill" style={{ width: `${(stats.Success+stats.Fail) / stats.ToDo * 100}%` }}></div>
                        </div>
                    </td>
                </tr>
                <tr>
                    <td>Success (current):</td>
                    <td>{stats.Success} ({stats.Success / stats.ToDo * 100}%)</td>
                </tr>
                <tr>
                    <td>Fail (current):</td>
                    <td>{stats.Fail} ({stats.Fail / stats.ToDo * 100}%)</td>
                </tr><tr>
                    <td>Success:</td>
                    <td>{stats.SuccessAll} ({stats.SuccessAll / stats.ToDo * 100}%)</td>
                </tr>
                <tr>
                    <td>Fail:</td>
                    <td>{stats.FailAll} ({stats.FailAll / stats.ToDo * 100}%)</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function WorkerStats({stats}) {
    return (
        <div>
            <h2>Worker Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Use:</td>
                    <td>{stats.InFinalize+stats.InCommP} / {stats.Available}</td>
                </tr>
                <tr>
                    <td colSpan={2}>
                        <div className="progress-bar">
                            <div className="progress-bar__fill" style={{ width: `${(stats.InFinalize+stats.InCommP) / stats.Available * 100}%` }}></div>
                        </div>
                    </td>
                </tr>
                <tr>
                    <td>Group Finalize:</td>
                    <td>{stats.InFinalize}</td>
                </tr>
                <tr>
                    <td>Compute Data CID</td>
                    <td>{stats.InCommP}</td>
                </tr><tr>
                    <td>Queued Tasks:</td>
                    <td>{stats.TaskQueue}</td>
                </tr>
                <tr>
                    <td>DataCID rate:</td>
                    <td>{formatBytesBinary(stats.CommPBytes)}</td>
                </tr>
                </tbody>
            </table>
        </div>
    )
}


// read/write busy time

// process stats [rpc]
// - fds
// - goroutines

// lotus rpc
// - calls

// - metamask integ?

// - market balance setting

// - commp compute rate
// - bsst compute rate??
// - local index rates

function Status() {
    const [walletInfo, setWalletInfo] = useState(null);
    const [groups, setGroups] = useState([]);
    const [crawlState, setCrawlState] = useState("");
    const [carUploadStats, setCarUploadStats] = useState({});
    const [reachableProviders, setReachableProviders] = useState([]);
    const [dealSummary, setDealSummary] = useState({});
    const [retrStats, setRetrStats] = useState({});
    const [stagingStats, setStagingStats] = useState({});
    const [retrChecker, setRetrChecker] = useState({})
    const [workerStats, setWorkerStats] = useState({})

    const fetchStatus = async () => {
        try {
            const walletInfo = await RibsRPC.call("WalletInfo");
            setWalletInfo(walletInfo);
            const groups = await RibsRPC.call("Groups");
            const crawlState = await RibsRPC.call("CrawlState");
            const carUploadStats = await RibsRPC.call("CarUploadStats");
            const reachableProviders = await RibsRPC.call("ReachableProviders");
            const dealSummary = await RibsRPC.call("DealSummary");
            const retrStats = await RibsRPC.call("RetrStats");
            const stagingStats = await RibsRPC.call("StagingStats");
            const retrCheckerStats = await RibsRPC.call("RetrChecker")
            const workerStats = await RibsRPC.call("WorkerStats")

            setGroups(groups);
            setCrawlState(crawlState);
            setCarUploadStats(carUploadStats);
            setReachableProviders(reachableProviders);
            setDealSummary(dealSummary);
            setRetrStats(retrStats);
            setStagingStats(stagingStats);
            setRetrChecker(retrCheckerStats);
            setWorkerStats(workerStats);
        } catch (error) {
            console.error("Error fetching status:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div className="">
            <div className="Status">
                <h1>Storage</h1>
                <div className="status-grid">
                    <GroupsTile groups={groups} />
                    <DealsTile dealSummary={dealSummary} />
                    <IoStats />
                    <TopIndexTile />
                </div>

                <h1><abbr title="Decentralized Storage Network">DSN</abbr></h1>
                <div className="status-grid">
                    <ProvidersTile reachableProviders={reachableProviders} />
                    <CarUploadStatsTile carUploadStats={carUploadStats} />
                    <CrawlStateTile crawlState={crawlState} />
                    <WalletInfoTile walletInfo={walletInfo} />
                </div>

                <h1>External Storage</h1>
                <div className="status-grid">
                    <RetrStats retrStats={retrStats} />
                    <StagingStats stagingStats={stagingStats} />
                    <RetrCheckerStats stats={retrChecker} />
                </div>

                <h1>Internals</h1>
                <div className="status-grid">
                    <P2PNodes />
                    <GoRuntimeStats />
                    <WorkerStats stats={workerStats} />
                </div>
            </div>
        </div>
    );
}

export default Status;
