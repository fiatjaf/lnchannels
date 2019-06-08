/** @format */

writeTable('longest-living', ['s', 'o', 'd'], window.longestliving)
writeTable('shortest-living', ['s', 'o', 'd'], window.shortestliving)

function writeTable(id, keys, data) {
  var table = document.getElementById(id)
  for (let i = 0; i < data.length; i++) {
    var row = document.createElement('tr')

    let datapoint = data[i]
    keys.forEach(attr => {
      var td = document.createElement('td')
      td.textContent = datapoint[attr]
      td.className = 'tr'
      row.appendChild(td)
    })

    table.appendChild(row)
  }
}
