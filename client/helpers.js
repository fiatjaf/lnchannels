/** @format */

export function abbr(id) {
  try {
    return id.slice(0, 4) + '…' + id.slice(-4)
  } catch (err) {
    return ''
  }
}

export const plotOptions = {
  series: {
    turboThreshold: 1,
    marker: {enabled: false}
  }
}

export function date(d) {
  try {
    return (new Date(d))
      .toISOString()
      .replace(/T/, ' ')
      .replace(/\..+/, '')
  } catch (err) {
    return ''
  }
}
