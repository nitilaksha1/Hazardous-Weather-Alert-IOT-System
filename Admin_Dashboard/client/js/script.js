var geoCoder;
var directionsDisplay;
var directionsService;
var myPos;
var placeService;
var markers = [];

function initMap() {
    console.log("func: initMap()");

    // set the center and zoom level of the map
    var mapProp = {
        center: new google.maps.LatLng(44.9727, -93.23540000000003),
        zoom: 13,
    };

    // create the map
    map = new google.maps.Map(document.getElementById('map'), mapProp);

    //  create a new instance of geoCoder
    geoCoder = new google.maps.Geocoder();

    var socket = io();
    socket.emit('dashboard_on');

    var set = new Set();

    // {"latitude":44.9723,"longitude":93.2625,"temperature":67.0,"windspeed":11.0,"visibility":1.0,"weatherAlert":"Zephyros has been kind. 2"}
    // {"latitude":44.9723,"longitude":93.2625,"temperature":67.0,"windspeed":11.0,"visibility":1.0,"weatherAlert":"Zephyros has been kind. 3"}

    socket.on('alert', function(data) {
        var eventArray = data.split(/\r?\n/);

        for(var i = 0; i < eventArray.length; i++) {
          jsonObject = JSON.parse(eventArray[i]);
          console.log("jsonObject.type= ", jsonObject.type);
          if(jsonObject.type === "car") {
            if(set.has(jsonObject.uuid)) {
              continue;
            } else {
              set.add(jsonObject.sensorId);
              console.log("jsonObject= ", jsonObject);
              var myLatLng = {lat: parseFloat(jsonObject.latitude), lng: parseFloat(jsonObject.longitude)};
              console.log("myLatLng= ", myLatLng);
              createMarkerV3(myLatLng);
            }
          } else {
            if(set.has(jsonObject.sensorId)) {
              continue;
            } else {
              set.add(jsonObject.sensorId);
              console.log("jsonObject= ", jsonObject);
              var myLatLng = {lat: jsonObject.latitude, lng: jsonObject.longitude};
              console.log("myLatLng= ", myLatLng);
              createMarkerV1(myLatLng, jsonObject.weatherAlert, jsonObject.temperature, jsonObject.windspeed, jsonObject.visibility);
            }
          }
        }
    });
}

function createMarkerV1(myLatLng, weatherAlert, temp, windspeed, visibility) {
  console.log("func: createMarkerV1()");

  var textToDisplay = '<div style="text-align: center">' + weatherAlert + '<br/>' +
                      'Temperature: ' + temp + '<br/>' +
                      'Wind Speed: ' + windspeed + '<br/>' +
                      'Visibility: ' + visibility + '<br/>' +
                      '</div>';

  var infowindow = new google.maps.InfoWindow({
      content: textToDisplay,
      maxWidth: 300
  });

  var icon = {
      url: '/client/img/marker.png', // url
      scaledSize: new google.maps.Size(40, 40), // scaled size
      origin: new google.maps.Point(0,0), // origin
      anchor: new google.maps.Point(0, 0) // anchor
  };

  var marker = new google.maps.Marker({
    position: myLatLng,
    map: map,
    icon: icon,
    animation: google.maps.Animation.BOUNCE,
    title: 'Hello World!'
  });

  // Add circle overlay and bind to marker
  var circle = new google.maps.Circle({
    map: map,
    radius: 1600,    // 10 miles in metres
    fillColor: '#AA0000'
  });

  circle.bindTo('center', marker, 'position');

  google.maps.event.addListener(marker, 'click', function() {
    infowindow.open(map, this);
  });
}

function createMarkerV3(myLatLng) {
  console.log("func: createMarkerV3()");
  var icon = {
      url: '/client/img/car2.png', // url
      scaledSize: new google.maps.Size(40, 40), // scaled size
      origin: new google.maps.Point(0,0), // origin
      anchor: new google.maps.Point(0, 0) // anchor
  };

  var marker = new google.maps.Marker({
    position: myLatLng,
    map: map,
    icon: icon,
    title: 'Hello World!'
  });
}
