/** @format */

const Chart = window.Chart

const chartColors = {
  red: 'rgb(255, 99, 132)',
  orange: 'rgb(255, 159, 64)',
  yellow: 'rgb(255, 205, 86)',
  green: 'rgb(75, 192, 192)',
  blue: 'rgb(54, 162, 235)',
  purple: 'rgba(153, 102, 255, 0.5)',
  grey: 'rgba(201, 203, 207, 0.4)'
}
new Chart(document.getElementById('open-close'), {
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
