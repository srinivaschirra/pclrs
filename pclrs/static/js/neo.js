var lat;
var lng;
var map;
var url = "";
var fls = [];
var interval = 0;
var xhttp = new XMLHttpRequest();
// this is the default duration
var duration = 'thisd';
var valid_durations = [ 'hr', 'dy', 'wk', 'mn', 'yr', 'at']
var default_mbox_layers = [];
// TODO: the invalid geojson object needs to be handled
// TODO: match the mapbox ui with the paleo google map style
// TODO: the pingleaf popup isn't working properly
// TODO: add one event listener to zoom and move end events
// TODO: check the gps html5 ip address default gps coordinates thoroughly
// TODO: the just created ping needs to be shown for 5 minutes in its original size even when you zoom in/out
// TODO: pings loaded counter needs to be updated whenever you create the ping
// TODO: all the pings data need to be refreshed with freshly created pings


function pleaf(){
    return  "<div class='ping-leap'><div class='ping-leap0'>" +
  "<div class='modal-dialog' style='width:350px;margin: -10px 5px 0px 4px;'>" +
    "<div class='modal-content'>" +
      "<div class='modal-header' style='padding:4px;'>" +
        "<button type='button' class='close ping1' data-dismiss='modal'> " +
          "<span aria-hidden='true'>×</span><span class='sr-only'>Close</span>" +
        "</button>" +
        "<div class='col-md-3'><img class='img-responsive' src='static/images/love.png' alt='Logo'></div>" +
        "<div class='col-md-8'>" +
         " <h4 class='modal-title' id='lineModalLabel' style='color:#868282;text-align:left'>Pray</h4></div>" +
      "</div>" +
      "<div class='modal-body' style='padding: 0px 6px;'>" +
        "<form>" +
          "<div class='form-group' style='text-align:left'>" +
            "<label for='comment' style='margin-left: 13px;margin-top:4px;color:#868282;text-align:left'>Description:</label>" +
            "<textarea class='form-control ping-form' rows='2' id='comment' placeholder='Describe more about how you feel...' style='outline: none;resize: none'></textarea>" +
          "</div>" +
        "</form>" +
      "</div>" +
      "<div class='modal-footer' style='padding:0px;'   id=''>" +
        "<div class='btn-group btn-group-justified' style='color:#868282' role='group' aria-label='group button'>"+
          "<div class=' col-md-6' role='group' style=' border-right: 1px solid #b7aaaa69;padding: 2px;text-align:center'>"+
            "<div class='col-md-2 padding-less'><i class='fa fa-compass' aria-hidden='true'></i></div>"+
            "<div class='col-md-9 padding-less'>Location Accuracy</div>"+
          "</div>"+
          "<div class=' col-md-6' role='group'>"+
            "<div class='col-md-2 padding-less'><i class='fa fa-user-secret fa-2x' aria-hidden='true'></i></div>"+
            "<div class='col-md-6 padding-less text-center'>Make Anonymous</div>"+
            "<div id='saveImage' class='col-md-3 padding-less' data-action='save' role='button'>"+
              "<button type='button' class='btn-ping' click='clickfunction()'>Save</button>"+
    "</div></div></div></div></div></div></div></div>";
}

$(document).ready(function() {
    $('.sbtn').click(function(e){
        // The following 4 lines need to go into a function
        var bnds = map.getBounds();
        var swlatlon = bnds.getSouthWest().wrap();
        var nelatlon = bnds.getNorthEast().wrap();
        var bbox = [swlatlon.lat, swlatlon.lng, nelatlon.lat, nelatlon.lng];
        //close the semi circle
        $('.plusbtn').toggleClass('open');
        $('.social-button').toggleClass('active');
        $.ajax({
            url: '/api/v1/ping/create',
            type: 'POST',
            data: JSON.stringify({'id': e.currentTarget.getAttribute('id'),
                                  'd': duration,
                                  'swlat': bbox[0],
                                  'swlon': bbox[1],
                                  'nelat': bbox[2],
                                  'nelon': bbox[3]}),
            contentType: 'application/json; charset=utf-8',
            dataType: 'json',
            success: function(data, status){
                L.marker([data.lat, data.lng]).addTo(map).setIcon(new L.icon({iconUrl: '/static/images/bigicons/'+data.iurl+'.png', iconSize: [60, 60]}));
                L.popup().setLatLng([data.lat, data.lng]).setContent(pleaf());

            },
            async: false});

        //create a large ping icon and open the pingleaf next to it
        // wire up the save button in the pingleaf
    });
    // Social plus button function
    $('.plus-button').click(function() {
        $('.plusbtn').toggleClass('open');
        $('.social-button').toggleClass('active');
    });
});
function duration_handler(dura){
    if (valid_durations.indexOf(dura) >= 0){
        switch(dura){
        case 'hr':
            duration = 'thish';
            break;
        case 'dy':
            duration = 'thisd';
            break;
        case 'wk':
            duration = 'thisw';
            break;
        case 'mn':
            duration = 'thism';
            break;
        case 'yr':
            duration = 'thisy';
            break;
        case 'at':
            duration = 'alltime';
        }
        loadLayer();
    }
}
function showError(er){
    // try to get the data from backend via ip address
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var obj = JSON.parse(this.responseText);
            lat = obj.data.lat;
            lng = obj.data.lng;
        }
    };
    xhttp.open('GET', '/api/v1/user/geoinfo?t=' + Math.random(), true)
    xhttp.send();
}
if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(function(position){
        lat = position.coords.latitude;
        lng = position.coords.longitude;
    }, showError);
} else {
    console.log("Geolocation is not supported by this browser.");
    showError('er');
}

function chkLatLng(){
    if (lat != undefined && lng != undefined){
        clearInterval(interval)
        initMap();
    }
}

function loadLayer(){
    var bnds = map.getBounds();
    var swlatlon = bnds.getSouthWest().wrap();
    var nelatlon = bnds.getNorthEast().wrap();
    var bbox = [swlatlon.lat, swlatlon.lng, nelatlon.lat, nelatlon.lng];
    url = '/api/v1/pings/map/' + duration + '/' + bbox[0] + '/' + bbox[1] + '/' + bbox[2] + '/' + bbox[3];
    map.eachLayer(function(layer){
        if (default_mbox_layers.indexOf(layer._leaflet_id) < 0){
            map.removeLayer(layer);
        }
    });
    var featureLayer = L.mapbox.featureLayer().loadURL(url).on('ready', function(e) {
        try{
            //update the footer with ping count
            document.getElementById('ftr').innerHTML = "Total Pings Loaded: " + Object.keys(e.target._layers).length;
            var clusterGroup = new L.MarkerClusterGroup({showCoverageOnHover: false, animateAddingMarkers: true});
            clusterGroup.on('click', function(a){
                console.log('marker' + a.layer.feature.properties.id);
            });
            clusterGroup.on('clusterclick', function(a){
                console.log('marker' + a.layer.getAllChildMarkers().length);
            });
            e.target.eachLayer(function(layer) {
                layer.setIcon(new L.icon({iconUrl: layer.feature.properties.iconUrl}));
                layer.on('click', function(e){
                    //e.bindPopup("hello");
                    $("#pleaf").show();
                    layer.bindPopup(document.getElementById('pleaf'));
                });
                clusterGroup.addLayer(layer);
            });
            map.addLayer(clusterGroup);
        }
        catch(err){
            console.log("Got invalid geojson object");
        }
    });
}



function initMap(){
    L.mapbox.accessToken = 'pk.eyJ1IjoiYmFydHBpbmciLCJhIjoiY2pheTQ4Nm85MTQzejJxcWs4dWc5Yzh3cyJ9.0UyqGF5cYVmWnWmVodADYQ';
    map = L.mapbox.map('map', 'mapbox.dark', {minZoom: 4,
                                              zoomControl: false,
                                              worldCopyJump: true,
                                              attributionControl: false,
                                              dragRotate: false}).setView([lat, lng], 8);
    map.eachLayer(function(lyer){
        default_mbox_layers.push(lyer._leaflet_id);
    });
    loadLayer();
    map.on('zoomend', function(e){
        loadLayer();});
    map.on('moveend', function(e){
        loadLayer();});
}

interval = setInterval(chkLatLng, 1000);
