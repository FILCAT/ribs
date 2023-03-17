function formatBytesBinary(bytes) {
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let l = 0, n = parseInt(bytes, 10) || 0;
    while (n >= 1024 && ++l) {
        n = n / 1024;
    }
    return (n.toFixed(n < 10 && l > 0 ? 1 : 0) + ' ' + units[l]);
}

function formatNum(bytes) {
    const units = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    let l = 0, n = parseInt(bytes, 10) || 0;
    while (n >= 1000 && ++l) {
        n = n / 1000;
    }
    return (n.toFixed(n < 10 && l > 0 ? 1 : 0) + ' ' + units[l]);
}

function formatPercent(num) {
    return (num * 100).toFixed(1) + '%';
}

function formatFil(n) {
    if (n === 0) {
        return '0';
    }

    const units = ['aFIL', 'fFIL', 'pFIL', 'nFIL', 'uFIL', 'mFIL', 'FIL'];
    let l = 0;
    while (l+1 < units.length && n >= 1000) {
        l++;
        n = n / 1000;
    }

    return (n.toFixed(n < 10 && l > 0 ? 1 : 0) + ' ' + units[l]);
}

const avgMonthDays = 30.436875;
const epochToMonth = (60/30) * 60 * 24 * avgMonthDays;
