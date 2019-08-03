/** @format */

var Chart = window.Chart

var chartColors = {
  red: 'rgb(255, 99, 132)',
  orange: 'rgb(255, 159, 64)',
  yellow: 'rgb(255, 205, 86)',
  green: 'rgb(75, 192, 192)',
  blue: 'rgb(54, 162, 235)',
  purple: 'rgba(153, 102, 255, 0.5)',
  grey: 'rgba(201, 203, 207, 0.4)'
}

// -- index.html
var openClose = document.getElementById('open-close')
if (openClose) {
  new Chart(openClose, {
    type: 'line',
    data: {
      labels: window.blocks,
      datasets: [
        {
          label: 'Opened channels',
          backgroundColor: chartColors.blue,
          borderColor: chartColors.blue,
          data: window.openings,
          fill: false,
          pointRadius: 1,
          yAxisID: 'change'
        },
        {
          label: 'Closed channels',
          backgroundColor: chartColors.red,
          borderColor: chartColors.red,
          data: window.closings,
          fill: false,
          pointRadius: 1,
          yAxisID: 'change'
        },
        {
          label: 'Total channels',
          backgroundColor: chartColors.purple,
          borderColor: chartColors.purple,
          data: window.total,
          fill: true,
          pointRadius: 0,
          yAxisID: 'acc'
        },
        {
          label: 'Total capacity (bitcoin)',
          backgroundColor: chartColors.grey,
          borderColor: chartColors.grey,
          data: window.capacity,
          fill: true,
          pointRadius: 0,
          yAxisID: 'cap'
        }
      ]
    },
    options: {
      responsive: true,
      title: {
        display: true,
        text: 'Channel variation'
      },
      tooltips: {
        mode: 'index',
        intersect: false
      },
      hover: {
        mode: 'nearest',
        intersect: true
      },
      scales: {
        xAxes: [
          {
            display: true,
            scaleLabel: {
              display: true,
              labelString: 'Block number (every 100)'
            }
          }
        ],
        yAxes: [
          {
            id: 'acc',
            type: 'linear',
            display: true,
            position: 'right',
            scaleLabel: {
              display: true,
              labelString: 'Total channels'
            }
          },
          {
            id: 'cap',
            type: 'linear',
            display: false
          },
          {
            id: 'change',
            type: 'logarithmic',
            display: true,
            position: 'left',
            scaleLabel: {
              display: true,
              labelString: 'Channel open/closes'
            },
            ticks: {
              callback: function(value, index, values) {
                if (
                  index === 0 ||
                  index === values.length - 1 ||
                  value.toString()[0] === '1'
                ) {
                  return '' + value
                }
              }
            }
          }
        ]
      }
    }
  })
}

// -- node.html
var nodeHistory = document.getElementById('node-channels-history')
if (nodeHistory) {
  var blockmap = {}
  var opens = {}
  var closes = {}
  var open_sats = {}
  var close_sats = {}

  var rows = document.querySelectorAll('table.node-channels-history tbody tr')
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i]
    var satoshis = parseFloat(row.children[2].innerHTML)
    var opened_at = parseInt(row.children[3].innerHTML)
    var closed_at = parseInt(row.children[4].innerHTML)

    opens[opened_at] = opens[opened_at] || 0
    open_sats[opened_at] = open_sats[opened_at] || 0
    opens[opened_at]++
    open_sats[opened_at] += satoshis
    blockmap[opened_at] = true

    if (!isNaN(closed_at)) {
      closes[opened_at] = closes[opened_at] || 0
      close_sats[opened_at] = close_sats[opened_at] || 0
      closes[opened_at]++
      close_sats[opened_at] += satoshis
      blockmap[closed_at] = true
    }
  }

  var blocks = Object.keys(blockmap).sort()
  var openings = []
  var closings = []
  var total = []
  var capacity = []

  i = 0
  for (var b in blockmap) {
    openings.push(opens[b])
    closings.push(closes[b])
    total.push(
      (total.length > 0 ? total[i - 1] : 0) + (opens[b] || 0) - (closes[b] || 0)
    )
    capacity.push(
      (capacity.length > 0 ? capacity[i - 1] : 0) +
        (open_sats[b] || 0) -
        (close_sats[b] || 0)
    )
    i++
  }

  new Chart(nodeHistory, {
    type: 'line',
    data: {
      labels: blocks,
      datasets: [
        {
          label: 'Opened channels',
          backgroundColor: chartColors.blue,
          borderColor: chartColors.blue,
          data: openings,
          fill: false,
          pointRadius: 1,
          yAxisID: 'change'
        },
        {
          label: 'Closed channels',
          backgroundColor: chartColors.red,
          borderColor: chartColors.red,
          data: closings,
          fill: false,
          pointRadius: 1,
          yAxisID: 'change'
        },
        {
          label: 'Total channels',
          backgroundColor: chartColors.purple,
          borderColor: chartColors.purple,
          data: total,
          fill: true,
          pointRadius: 0,
          yAxisID: 'acc'
        },
        {
          label: 'Total capacity (bitcoin)',
          backgroundColor: chartColors.grey,
          borderColor: chartColors.grey,
          data: capacity,
          fill: true,
          pointRadius: 0,
          yAxisID: 'cap'
        }
      ]
    },
    options: {
      responsive: true,
      title: {
        display: true,
        text: 'Channel variation'
      },
      tooltips: {
        mode: 'index',
        intersect: false
      },
      hover: {
        mode: 'nearest',
        intersect: true
      },
      scales: {
        xAxes: [
          {
            display: true,
            scaleLabel: {
              display: true,
              labelString: 'Block number (every 100)'
            }
          }
        ],
        yAxes: [
          {
            id: 'acc',
            type: 'linear',
            display: true,
            position: 'right',
            scaleLabel: {
              display: true,
              labelString: 'Total channels'
            }
          },
          {
            id: 'cap',
            type: 'linear',
            display: false
          },
          {
            id: 'change',
            type: 'logarithmic',
            display: true,
            position: 'left',
            scaleLabel: {
              display: true,
              labelString: 'Channel open/closes'
            },
            ticks: {
              callback: function(value, index, values) {
                if (
                  index === 0 ||
                  index === values.length - 1 ||
                  value.toString()[0] === '1'
                ) {
                  return '' + value
                }
              }
            }
          }
        ]
      }
    }
  })
}
