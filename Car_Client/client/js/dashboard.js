// -------COUNT-------

// function
var countItDown = function() {
  var countdown = document.getElementById("countdown");
  var currentTime = parseFloat(countdown.textContent);
  if (currentTime < 65) {
    countdown.textContent = currentTime + 1;
  } else {
    countdown.textContent = 56;
  }

};
// call interval 
var timer = window.setInterval(countItDown, 100);

// ----- On render -----
$(function() {

  makeRadial({
    el: $('#radial'),
    radials: 100
  })
});

function makeRadial(options) {
  if (options && options.el) {
    var el = options.el;
    var radials = 60;
    if (options.radials) {
      radials = options.radials;
    }
    var degrees = 360 / radials;
    var i = 0;
    for (i = 0; i < (radials / 2); i++) {
      var newTick = $('<div class="tick"></div>').css({
        '-moz-transform': 'rotate(' + (i * degrees) + 'deg)'
      }).css({
        '-webkit-transform': 'rotate(' + (i * degrees) + 'deg)'
      }).css({
        'transform': 'rotate(' + (i * degrees) + 'deg)'
      })
      el.prepend(newTick);
    }
  }
}



