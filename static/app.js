/** @format */

// -- global
const H = window.Highcharts
const AutoComplete = window.autoComplete
const fetch = window.fetch

const plotOptions = {
  series: {
    turboThreshold: 1,
    marker: {enabled: false}
  }
}

// only do anything after 'load'
window.addEventListener('load', () => {
  // -- base.html
  new AutoComplete({
    selector: '#search',
    source(term, response) {
      if (term.length < 3) response([])

      fetch('/search?q=' + encodeURIComponent(term))
        .then(r => r.json())
        .then(res => response(res.results))
    },
    renderItem({closed, url, label, kind}) {
      return `<div class="search-item ${closed ? 'closed' : ''}">
        <a href="${url}">${kind === 'node' ? '⏺' : '⤢'} ${label}</a>
    </div>`
    },
    cache: true
  })

  // -- index.html
  if (document.getElementById('open-close')) {
    H.chart('open-close', {
      title: {text: ''},
      xAxis: {
        categories: window.blocks.map(b => b.toString().slice(0, -2) + '__')
      },
      yAxis: [
        {visible: false},
        {visible: false},
        {visible: false},
        {visible: false},
        {visible: false}
      ],
      series: [
        {
          name: 'capacity (btc)',
          type: 'area',
          data: window.capacity,
          step: 'left',
          yAxis: 2,
          color: 'var(--gold)'
        },
        {
          name: 'total',
          type: 'area',
          data: window.total,
          step: 'left',
          yAxis: 1,
          color: 'var(--blue)'
        },
        {
          name: 'openings',
          type: 'column',
          data: window.openings,
          yAxis: 0,
          color: 'var(--green)',
          borderWidth: 1
        },
        {
          name: 'closings',
          type: 'column',
          data: window.closings,
          yAxis: 0,
          color: 'var(--red)',
          borderWidth: 1
        },
        {
          name: 'fee total (sat)',
          type: 'spline',
          data: window.fee_total,
          yAxis: 3,
          color: 'rgba(0, 0, 0, 0.5)',
          lineWidth: 1,
          visible: false
        },
        {
          name: 'fee average (sat/n)',
          type: 'spline',
          data: window.fee_average,
          yAxis: 4,
          color: 'rgba(0, 0, 0, 0.7)',
          dashStyle: 'Dash',
          lineWidth: 1,
          visible: false
        }
      ],
      plotOptions
    })
  }

  // -- node.html
  if (document.getElementById('node-channels-history')) {
    let table = document.querySelector('table.node-channels-history')

    // toggle incoming-outgoing fee policy display
    let toggles = table.querySelectorAll('a.toggle-outgoing-incoming')
    for (let i = 0; i < toggles.length; i++) {
      let toggle = toggles[i]
      toggle.addEventListener('click', e => {
        e.preventDefault()
        table.classList.toggle('show-outgoing')
        table.classList.toggle('show-incoming')
      })
    }

    // gather info
    var blockmap = {}
    var opens = {}
    var closes = {}
    var open_sats = {}
    var close_sats = {}
    var maxfee = 0
    var maxcap = 0
    var openchannelsbubbles = []

    let rows = table.querySelectorAll('tbody tr')
    for (let i = rows.length - 1; i >= 0; i--) {
      let row = rows[i]
      let satoshis = parseFloat(row.children[2].innerHTML)
      let opened_at = parseInt(row.children[4].innerHTML)
      let closed_at = parseInt(row.children[5].innerHTML.split(' ')[0])
      let peer_name = row.children[0].textContent
      let peer_size = parseInt(row.children[0].dataset.size)
      let peer_url = row.children[0].children[0].href

      // gather data for the chart
      opens[opened_at] = opens[opened_at] || 0
      open_sats[opened_at] = open_sats[opened_at] || 0
      opens[opened_at]++
      open_sats[opened_at] += satoshis
      blockmap[opened_at] = true

      if (!isNaN(closed_at)) {
        // if it's closed gather close data
        closes[closed_at] = closes[closed_at] || 0
        close_sats[closed_at] = close_sats[closed_at] || 0
        closes[closed_at]++
        close_sats[closed_at] += satoshis
        blockmap[closed_at] = true
      } else {
        // if it's open add to bubble chart
        openchannelsbubbles.push({
          x: opened_at,
          y: satoshis,
          z: peer_size,
          name: peer_name,
          url: peer_url
        })
      }

      // data for the microcharts later
      let fee = parseInt(row.children[3].innerHTML.split(' ').slice(-1)[0])
      maxfee = fee > maxfee ? fee : maxfee
      let cap = parseInt(row.children[2].innerHTML)
      maxcap = cap > maxcap ? cap : maxcap
    }

    // insert last block if not exists (so the chart is not stuck in a past position)
    if (!(window.stats.last_block in blockmap)) {
      blockmap[window.stats.last_block] = true
    }

    // make main chart
    let blocks = Object.keys(blockmap).sort()
    var openings = []
    var closings = []
    var total = []
    var capacity = []

    for (let i = 0; i < blocks.length; i++) {
      let b = blocks[i]
      let x = parseInt(b)
      openings.push([x, opens[b] || 0])
      closings.push([x, closes[b] || 0])
      total.push([
        x,
        (total.length > 0 ? total[i - 1][1] : 0) +
          (opens[b] || 0) -
          (closes[b] || 0)
      ])
      capacity.push([
        x,
        (capacity.length > 0 ? capacity[i - 1][1] : 0) +
          (open_sats[b] || 0) -
          (close_sats[b] || 0)
      ])
    }

    H.chart('node-channels-history', {
      title: {text: ''},
      yAxis: [{visible: false}, {visible: false}, {visible: false}],
      series: [
        {
          name: 'capacity (sat)',
          type: 'area',
          data: capacity,
          step: 'left',
          yAxis: 2,
          color: 'var(--gold)'
        },
        {
          name: 'total',
          type: 'area',
          data: total,
          step: 'left',
          yAxis: 1,
          color: 'var(--blue)'
        },
        {
          name: 'openings',
          type: 'column',
          data: openings,
          yAxis: 0,
          color: 'var(--green)',
          borderWidth: 1
        },
        {
          name: 'closings',
          type: 'column',
          data: closings,
          yAxis: 0,
          color: 'var(--red)',
          borderWidth: 1
        }
      ],
      plotOptions
    })

    // create microcharts
    for (let i = 0; i < rows.length; i++) {
      let row = rows[i]

      // fee
      let fee = parseInt(row.children[3].innerHTML.split(' ').slice(-1)[0])
      let feescaled = 100 * (fee / maxfee)
      let fw = feescaled.toFixed(2)
      row.children[3].innerHTML += `<i class="bar" style="width:${fw}%; background: var(--gold)" />`

      // capacity
      let capscaled = (100 * parseInt(row.children[2].innerHTML)) / maxcap
      let cw = capscaled.toFixed(2)
      row.children[2].innerHTML += `<i class="bar" style="width:${cw}%; background: var(--gold)" />`
    }

    // channel bubbles
    H.chart('node-channels-bubble', {
      title: {text: ''},
      yAxis: [{title: {text: 'channel size (sat)', enabled: null}, floor: 0}],
      series: [
        {
          type: 'bubble',
          data: openchannelsbubbles,
          marker: {fillColor: 'var(--gold)'},
          showInLegend: false,
          minSize: '1%',
          maxSize: '30%',
          sizeBy: 'width',
          dataLabels: {
            enabled: true,
            format: '{point.name}',
            style: {
              color: 'black',
              textOutline: 'none',
              fontWeight: 'normal'
            }
          },
          tooltip: {
            headerFormat: '',
            followPointer: true,
            followTouchMove: true,
            pointFormat: '{point.name}: {point.y}',
            valueSuffix: ' sat'
          },
          events: {
            click: e => {
              location.href = e.point.url
            }
          }
        }
      ]
    })
  }
})
