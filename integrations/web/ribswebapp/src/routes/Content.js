import React, {useEffect, useState} from 'react';
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatNum, epochToDate, epochToDuration } from "../helpers/fmt";
import { Group } from "./Groups";

function Content() {
    const [cid, setCid] = useState('');
    const [groupIds, setGroupIds] = useState(null);
    const [headHeight, setHeadHeight] = useState(0);
    const [expandedGroupId, setExpandedGroupId] = useState(null);

    const [error, setError] = useState(null);

    const findCid = async () => {
        if (cid !== '') {
            try {
                const result = await RibsRPC.call("FindCid", [{"/": cid}]);
                setError(null);
                setGroupIds(result);
                setExpandedGroupId(0);
            } catch (error) {
                setError(error);
                setGroupIds(null);
                return;
            }
        }
    };

    const fetchHead = async () => {
        try {
            const head = await RibsRPC.callFil("ChainHead");
            setHeadHeight(head.Height);
        } catch (error) {
            console.error("Error fetching head:", error);
        }
    };

    useEffect(() => {
        fetchHead();
        const intervalId = setInterval(fetchHead, 5000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    const handleGroupClick = (id) => {
        if (id === expandedGroupId) {
            setExpandedGroupId(null);
        } else {
            setExpandedGroupId(id);
        }
    }

    return (
        <div className="Content" style={{fontFamily: 'Arial, sans-serif', color: '#333'}}>
            <div>
                <h1 style={{textAlign: 'center'}}>Inspect Content</h1>
            </div>
            <div style={{display: 'flex', justifyContent: 'center', alignItems: 'center'}}>
                <button onClick={findCid} style={{marginRight: '1em', padding: '0.5em'}}>Find</button>
                <input type="text" placeholder="baf..." style={{width: '50em', padding: '0.5em', margin: '0.5em'}} onChange={(e) => setCid(e.target.value)} />
            </div>
            {groupIds === null ? null : groupIds.length > 0 ? (
                <div style={{margin: '2em'}}>
                    <h2 style={{textAlign: 'center'}}>Group IDs</h2>
                    <div>CID: <code>{cid}</code></div>
                    <ul style={{listStyleType: 'none', paddingLeft: 0}}>
                        {groupIds.map((id, index) => (
                            <li key={index} onClick={() => handleGroupClick(index)} style={{margin: '1em 0', cursor: 'pointer', padding: '1em', borderRadius: '10px', boxShadow: '0px 0px 10px rgba(0, 0, 0, 0.1)'}}>
                                {index !== expandedGroupId && <>Group {id}</>}
                                {index === expandedGroupId && <Group groupKey={id} headHeight={headHeight} />}
                            </li>
                        ))}
                    </ul>
                </div>
            ) : (
                <div>
                    <h2 style={{textAlign: 'center'}}>Not Found</h2>
                </div>
            )}
            {error !== null && (
                <div style={{margin: '2em'}}>
                    <h2 style={{textAlign: 'center'}}>Error</h2>
                    <div style={{margin: '1em 0', padding: '1em', borderRadius: '10px', boxShadow: '0px 0px 10px rgba(0, 0, 0, 0.1)'}}>
                        {error.message}
                    </div>
                </div>
            )}
        </div>
    );
}

export default Content;
