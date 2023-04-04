import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import RibsRPC from "../helpers/rpc";
import {formatBytesBinary, formatFil, epochToMonth, epochToDate, epochToDuration} from "../helpers/fmt";
import "./Status.css";
import "./Provider.css";

function Provider() {
    const [provider, setProvider] = useState(null);
    const [headHeight, setHeadHeight] = useState(0);
    const { providerID } = useParams();

    const fetchProvider = async () => {
        try {
            let prov = providerID.slice(2, providerID.length)
            prov = parseInt(prov)

            const provideData = await RibsRPC.call("ProviderInfo", [prov]);
            setProvider(provideData);

            const head = await RibsRPC.callFil("ChainHead");
            setHeadHeight(head.Height);
        } catch (error) {
            console.error("Error fetching providers:", error);
        }
    };

    useEffect(() => {
        fetchProvider();
        const intervalId = setInterval(fetchProvider, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    /*
    * type ProviderMeta struct {
	ID     int64
	PingOk bool

	BoostDeals     bool
	BoosterHttp    bool
	BoosterBitswap bool

	IndexedSuccess int64
	IndexedFail    int64

	DealStarted  int64
	DealSuccess  int64
	DealFail     int64
	DealRejected int64

	RetrProbeSuccess int64
	RetrProbeFail    int64
	RetrProbeBlocks  int64
	RetrProbeBytes   int64

	// price in fil/gib/epoch
	AskPrice         float64
	AskVerifiedPrice float64

	AskMinPieceSize float64
	AskMaxPieceSize float64
}
    *
    * */

    return (
        <div className="Provider">
            {provider && (
                <>
                    <h2>Provider {providerID}</h2>

                    <table className="compact-table provider-meta-table">
                        <tbody>
                        <tr>
                            <td>Ping OK</td>
                            <td>{provider.Meta.PingOk ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Boost Deals</td>
                            <td>{provider.Meta.BoostDeals ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Booster HTTP</td>
                            <td>{provider.Meta.BoosterHttp ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Booster Bitswap</td>
                            <td>{provider.Meta.BoosterBitswap ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Ask Price</td>
                            <td>{formatFil(provider.Meta.AskPrice)}</td>
                        </tr>
                        <tr>
                            <td>Ask Verified Price</td>
                            <td>{formatFil(provider.Meta.AskVerifiedPrice)}</td>
                        </tr>
                        <tr>
                            <td>Ask Min Piece Size</td>
                            <td>{formatBytesBinary(provider.Meta.AskMinPieceSize)}</td>
                        </tr>
                        <tr>
                            <td>Ask Max Piece Size</td>
                            <td>{formatBytesBinary(provider.Meta.AskMaxPieceSize)}</td>
                        </tr>
                        <tr>
                            <td>Indexed Success</td>
                            <td>{provider.Meta.IndexedSuccess}</td>
                        </tr>
                        <tr>
                            <td>Indexed Fail</td>
                            <td>{provider.Meta.IndexedFail}</td>
                        </tr>
                        <tr>
                            <td>Deal Started</td>
                            <td>{provider.Meta.DealStarted}</td>
                        </tr>
                        <tr>
                            <td>Deal Success</td>
                            <td>{provider.Meta.DealSuccess}</td>
                        </tr>
                        <tr>
                            <td>Deal Fail</td>
                            <td>{provider.Meta.DealFail}</td>
                        </tr>
                        <tr>
                            <td>Deal Rejected</td>
                            <td>{provider.Meta.DealRejected}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Success</td>
                            <td>{provider.Meta.RetrProbeSuccess}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Fail</td>
                            <td>{provider.Meta.RetrProbeFail}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Blocks</td>
                            <td>{provider.Meta.RetrProbeBlocks}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Bytes</td>
                            <td>{formatBytesBinary(provider.Meta.RetrProbeBytes)}</td>
                        </tr>

                        </tbody>
                    </table>

                    <h4>Recent Deals</h4>

                    <table className="compact-table providers-table">
                        <thead>
                        <tr>
                            <th>UUID</th>
                            <th>Status</th>
                            <th>Start Epoch</th>
                            <th>End Epoch</th>
                            <th>Error</th>
                            <th>Deal ID</th>
                            <th>Transferred</th>
                            <th>Publish CID</th>
                        </tr>
                        </thead>
                        <tbody>
                        {provider.RecentDeals && provider.RecentDeals.map((deal) => (
                            <tr key={deal.UUID}>

                                <td><abbr title={deal.UUID}>{deal.UUID.substring(0, 8)}... </abbr></td>
                                <td>{deal.Status}</td>
                                <td>{epochToDate(deal.StartEpoch)}, {epochToDuration(deal.StartEpoch-headHeight)}</td>
                                <td>{epochToDate(deal.EndEpoch)}, {epochToDuration(deal.EndEpoch-headHeight)}</td>
                                <td className="provider-deals-error-col">{deal.Error && <pre>{deal.Error}</pre>}</td>
                                <td>{deal.DealID && <a href={`https://filfox.info/en/deal/${deal.DealID}`} target="_blank" rel="noopener noreferrer">{deal.DealID}</a> || <></>}</td>
                                <td>{deal.BytesRecv && <>{deal.BytesRecv}/{deal.TxSize}</> || <></>}</td>
                                <td>{deal.PubCid && <a href={`https://filfox.info/en/message/${deal.PubCid}`} target="_blank" rel="noopener noreferrer">bafy..{deal.PubCid.substr(-16)}</a>}</td>
                            </tr>
                        ))}
                        </tbody>
                    </table>
                </>
            )}
        </div>
    );
}

export default Provider;