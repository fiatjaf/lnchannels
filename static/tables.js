/** @format */

var table = document.getElementById('longest-living')
for (let i = 0; i < window.longestliving.length; i++) {
  var row = document.createElement('tr')

  let channel = window.longestliving[i]
  ;['s', 'o', 'd'].forEach(attr => {
    var td = document.createElement('td')
    td.textContent = channel[attr]
    td.className = 'tr'
    row.appendChild(td)
  })

  table.appendChild(row)
}
