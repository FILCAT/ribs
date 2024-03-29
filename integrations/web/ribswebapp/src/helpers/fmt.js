export function formatBytesBinary(bytes) {
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let l = 0, n = parseInt(bytes, 10) || 0;
    while (n >= 1024 && ++l) {
        n = n / 1024;
    }
    return (n.toFixed(l > 0 ? 2 : 0) + ' ' + units[l]);
}

export function formatBitsBinary(bytes) {
    const units = ['bps', 'Kbps', 'Mbps', 'Gbps', 'Tbps', 'Pbps', 'Ebps', 'Zbps', 'Ybps'];
    let l = 0, n = parseInt(bytes*8, 10) || 0;
    while (n >= 1024 && ++l) {
        n = n / 1024;
    }
    return (n.toFixed(l > 0 ? 2 : 0) + ' ' + units[l]);
}

export function formatNum(nm, fracDigits = 0) {
    const units = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    let l = 0, n = parseFloat(nm) || 0;
    while (n >= 1000 && ++l) {
        n = n / 1000;
    }
    return (n.toFixed(l > 0 ? fracDigits : 0) + ' ' + units[l]).trim();
}

export function formatNum6(bytes) {
    const units = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    let l = 0, n = parseInt(bytes, 10) || 0;
    while (n >= 1000 && ++l) {
        n = n / 1000;
    }

    const intPart = Math.floor(n);
    const decimalPart = Math.floor((n - intPart) * 1000);
    const formattedDecimal = decimalPart > 0 ? `.${decimalPart}` : '';

    return (intPart + formattedDecimal + ' ' + units[l]);
}

export function calcEMA(currentValue, prevEMA, smoothingFactor) {
    return smoothingFactor * currentValue + (1 - smoothingFactor) * prevEMA;
}

export function formatPercent(num) {
    return (num * 100).toFixed(1) + '%';
}

export function formatFil(n) {
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

export function epochToDuration(epochs) {
    const epochDuration = 30; // 30 seconds per epoch
    const totalSeconds = Math.abs(epochs) * epochDuration;

    const daysInMonth = 30; // Approximate number of days in a month
    const months = Math.floor(totalSeconds / (daysInMonth * 24 * 60 * 60));
    const days = Math.floor((totalSeconds % (daysInMonth * 24 * 60 * 60)) / (24 * 60 * 60));
    const hours = Math.floor((totalSeconds % (24 * 60 * 60)) / (60 * 60));
    const minutes = Math.floor((totalSeconds % (60 * 60)) / 60);

    let duration = "";
    if (months > 0) {
        duration += `${months}mo `;
    }
    if (days > 0) {
        duration += `${days}d `;
    }
    if (hours > 0) {
        duration += `${hours}h `;
    }
    if (minutes > 0) {
        duration += `${minutes}m`;
    }
    return epochs < 0 ? `${duration.trim()} ago` : `in ${duration.trim()}`;
}

export function epochToDate(epochs, referenceDate = new Date(Date.UTC(2020, 9, 15, 22, 0, 0))) {
  const epochDuration = 30; // 30 seconds per epoch
  const totalSeconds = epochs * epochDuration;

  const newDate = new Date(referenceDate.getTime() + totalSeconds * 1000).toISOString().split('T')[0];
  return newDate;
}

export const avgMonthDays = 30.436875;
export const epochToMonth = (60/30) * 60 * 24 * avgMonthDays;

export function formatTimestamp(unixTimestamp, dateOnly) {
    const date = new Date(unixTimestamp * 1000);
    const formattedDate = date.toISOString().split('T')[0];

    if (dateOnly) {
        return formattedDate;
    }

    const now = new Date();
    let diff = date - now;
    let prefix = '', suffix = '';

    if (diff < 0) {
        suffix = " ago";
        diff = -diff;
    } else {
        prefix = "in ";
    }

    const daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    const isLeapYear = (year) => (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
    if (isLeapYear(date.getFullYear())) daysInMonth[1] = 29;

    const months = Math.floor(diff / (daysInMonth[date.getMonth()] * 24 * 60 * 60 * 1000));
    diff -= months * daysInMonth[date.getMonth()] * 24 * 60 * 60 * 1000;

    const days = Math.floor(diff / (24 * 60 * 60 * 1000));
    diff -= days * 24 * 60 * 60 * 1000;

    const hours = Math.floor(diff / (60 * 60 * 1000));
    diff -= hours * 60 * 60 * 1000;

    const minutes = Math.floor(diff / (60 * 1000));

    return `${formattedDate}, ${prefix}${months ? `${months}mo ` : ''}${days ? `${days}d ` : ''}${hours}h ${minutes}m${suffix}`;
}

