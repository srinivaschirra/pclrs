var lat;
var lng;
var map;
var url = "";
var fls = [];
var interval = 0;
var xhttp = new XMLHttpRequest();
// this is the default duration
var duration = 'thisd';
var valid_durations = ['hr', 'dy', 'wk', 'mn', 'yr', 'at']
var persona_infyscroll_cntlr;
var persona_infyscroll_scene;
var default_mbox_layers = [];
if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(function (position) {
        lat = position.coords.latitude;
        lng = position.coords.longitude;
    }, showError);
} else {
    console.log("Geolocation is not supported by this browser.");
    showError('er');
}
$("#circle-show").hover(function () {
    $("#circle-hide").toggle();
});
// Radialize the colors
Highcharts.setOptions({
    colors: Highcharts.map(Highcharts.getOptions().colors, function (color) {
        return {
            radialGradient: {
                cx: 0.5,
                cy: 0.3,
                r: 0.7
            },
            stops: [[0, color], [1, Highcharts.Color(color).brighten(-0.3).get('rgb')]]
        };
    })
});

function addBoxes(amount) {
    for (i = 1; i <= amount; i++) {
        $(pleaf(id)).addClass("box1").appendTo(".dynamicContent #content");
    }
    // "loading" done -> revert to normal state
    persona_infyscroll_scene.update(); // make sure the scene gets the new start position
    $("#loader").removeClass("active");
}
// persona page function
function persona() {
    // init controller
    persona_infyscroll_cntlr = new ScrollMagic.Controller();
    // build scene
    persona_infyscroll_scene = new ScrollMagic.Scene({
        triggerElement: ".dynamicContent #loader",
        triggerHook: "onEnter"
    }).addTo(persona_infyscroll_cntlr).on("enter", function (e) {
        if (!$("#loader").hasClass("active")) {
            $("#loader").addClass("active");
            if (console) {
                console.log("loading new items");
            }
            // simulate ajax call to add content using the function below
            setTimeout(addBoxes, 2000, 9);
        }
    });
    // pseudo function to add new content. In real life it would be done through an ajax request.
    // add some boxes to start with.
    addBoxes(2);
    $.ajax({
        url: '/api/v1/a/pings/graphs/wc/20170101/20180101',
        type: 'GET',
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        success: function (data, status) {
            var lines = data['data']['wc'].split(/[,\. ]+/g),
                data = Highcharts.reduce(lines, function (arr, word) {
                    var obj = Highcharts.find(arr, function (obj) {
                        return obj.name === word;
                    });
                    if (obj) {
                        obj.weight += 1;
                    } else {
                        obj = {
                            name: word,
                            weight: 1
                        };
                        arr.push(obj);
                    }
                    return arr;
                }, []);
            Highcharts.chart('wcchart', {
                series: [{
                    type: 'wordcloud',
                    data: data,
                    name: 'Frequency'
		}],
                title: {
                    text: 'Ping Cloud'
                }
            });
        },
        async: false
    });
    Highcharts.chart('persona_area', {
        chart: {
            type: 'area'
        },
        title: {
            text: 'Near By Ping Activity'
        },
        xAxis: {
            allowDecimals: false,
            labels: {
                formatter: function () {
                    return this.value;
                }
            }
        },
        yAxis: {
            title: {
                text: '# pings'
            },
            labels: {
                formatter: function () {
                    return this.value / 1000 + 'k';
                }
            }
        },
        tooltip: {
            pointFormat: '{series.name} produced <b>{point.y:,.0f}</b><br/>neighborhood in {point.x}'
        },
        plotOptions: {
            area: {
                pointStart: 2017,
                marker: {
                    enabled: false,
                    symbol: 'circle',
                    radius: 1,
                    states: {
                        hover: {
                            enabled: true
                        }
                    }
                }
            }
        },
        series: [{
            name: 'Pings',
            data: [1000, 13500, 14000, 1500, 16000, 20000, 18000, 9000, 5000, 4500, 11000, 14000]
    }]
    });
    // Build the chart
    Highcharts.chart('pchart', {
        chart: {
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false,
            type: 'pie'
        },
        title: {
            text: 'Ping Popularity January, 2017 to December, 2017'
        },
        tooltip: {
            pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
        },
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    format: '<b>{point.name}</b>: {point.percentage:.1f} %',
                    style: {
                        color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black'
                    },
                    connectorColor: 'silver'
                }
            }
        },
        series: [{
            name: 'Pings',
            data: [
                {
                    name: 'Thanks',
                    y: 25.95
                }
                , {
                    name: 'Happy',
                    y: 25.13,
                    sliced: true,
                    selected: true
                }
                , {
                    name: 'Love',
                    y: 24.61
                }
                , {
                    name: 'Sad',
                    y: 24.57
                }
                , {
                    name: 'Change',
                    y: 24.85
                }]
	}]
    });
    Highcharts.chart('bchart', {
        title: {
            text: 'Ping At a Glance'
        },
        xAxis: {
            categories: ['Q1', 'Q2', 'Q3', 'Q4']
        },
        series: [{
                type: 'column',
                name: 'Love',
                data: [3, 2, 1, 3]
            }
            , {
                type: 'column',
                name: 'Sad',
                data: [2, 3, 5, 7]
            }
            , {
                type: 'column',
                name: 'Change',
                data: [4, 3, 3, 9]
            }
            , {
                type: 'column',
                name: 'Thanks',
                data: [4, 3, 3, 9]
            }
            , {
                type: 'column',
                name: 'Happy',
                data: [4, 3, 3, 9]
            }
            , {
                type: 'spline',
                name: 'Average',
                data: [3, 2.67, 3, 6.33],
                marker: {
                    lineWidth: 2,
                    lineColor: Highcharts.getOptions().colors[3],
                    fillColor: 'white'
                }
		 }]
    });
}
// stats page function
function stats() {
    Highcharts.chart('stats_area', {
        chart: {
            type: 'area'
        },
        title: {
            text: 'Near By Ping Activity'
        },
        xAxis: {
            allowDecimals: false,
            labels: {
                formatter: function () {
                    return this.value;
                }
            }
        },
        yAxis: {
            title: {
                text: '# pings'
            },
            labels: {
                formatter: function () {
                    return this.value / 1000 + 'k';
                }
            }
        },
        tooltip: {
            pointFormat: '{series.name} produced <b>{point.y:,.0f}</b><br/>neighborhood in {point.x}'
        },
        plotOptions: {
            area: {
                pointStart: 2017,
                marker: {
                    enabled: false,
                    symbol: 'circle',
                    radius: 1,
                    states: {
                        hover: {
                            enabled: true
                        }
                    }
                }
            }
        },
        series: [{
            name: 'Pings',
            data: [1000, 13500, 14000, 1500, 16000, 20000, 18000, 9000, 5000, 4500, 11000, 14000]
	}]
    });
    // Build the chart
    Highcharts.chart('pchart', {
        chart: {
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false,
            type: 'pie'
        },
        title: {
            text: 'Ping Popularity January, 2017 to December, 2017'
        },
        tooltip: {
            pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
        },
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    format: '<b>{point.name}</b>: {point.percentage:.1f} %',
                    style: {
                        color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black'
                    },
                    connectorColor: 'silver'
                }
            }
        },
        series: [{
            name: 'Pings',
            data: [
                {
                    name: 'Thanks',
                    y: 25.95
                }
                , {
                    name: 'Happy',
                    y: 25.13,
                    sliced: true,
                    selected: true
                }
                , {
                    name: 'Love',
                    y: 24.61
                }
                , {
                    name: 'Sad',
                    y: 24.57
                }
                , {
                    name: 'Change',
                    y: 24.85
                }]
	}]
    });
    Highcharts.chart('bchart', {
        title: {
            text: 'Ping At a Glance'
        },
        xAxis: {
            categories: ['Q1', 'Q2', 'Q3', 'Q4']
        },
        series: [{
                type: 'column',
                name: 'Love',
                data: [3, 2, 1, 3]
            }
            , {
                type: 'column',
                name: 'Sad',
                data: [2, 3, 5, 7]
            }
            , {
                type: 'column',
                name: 'Change',
                data: [4, 3, 3, 9]
            }
            , {
                type: 'column',
                name: 'Thanks',
                data: [4, 3, 3, 9]
            }
            , {
                type: 'column',
                name: 'Happy',
                data: [4, 3, 3, 9]
            }
            , {
                type: 'spline',
                name: 'Average',
                data: [3, 2.67, 3, 6.33],
                marker: {
                    lineWidth: 2,
                    lineColor: Highcharts.getOptions().colors[3],
                    fillColor: 'white'
                }
            }]
    });
}
// home page function
function shwmp() {
    interval = setInterval(chkLatLng, 1000);
    $('.sbtn').click(function (e) {
        // The following 4 lines need to go into a function
        var bnds = map.getBounds();
        var swlatlon = bnds.getSouthWest().wrap();
        var nelatlon = bnds.getNorthEast().wrap();
        var bbox = [swlatlon.lat, swlatlon.lng, nelatlon.lat, nelatlon.lng];
        //close the semi circle
        $(function () {
            $('.plusbtn').toggleClass('open');
            $('.social-button').toggleClass('active');
        });
        $.ajax({
            url: '/api/v1/ping/create',
            type: 'POST',
            data: JSON.stringify({
                'id': e.currentTarget.getAttribute('id'),
                'd': duration,
                'swlat': bbox[0],
                'swlon': bbox[1],
                'nelat': bbox[2],
                'nelon': bbox[3]
            }),
            contentType: 'application/json; charset=utf-8',
            dataType: 'json',
            success: function (data, status) {
                L.marker([data.lat, data.lng]).
                addTo(map).
                setIcon(new L.icon({
                    iconUrl: '/static/images/bigicons/' + data.iurl + '.png',
                    iconSize: [60, 60]
                })).
                bindPopup(epleaf(data.id, data.iurl)).
                openPopup();
            },
            async: false
        });
    });
    $('.plus-button').click(function () {
        $('.plusbtn').toggleClass('open');
        $('.social-button').toggleClass('active');
    });
}

function epleaf(id, icon) {
    alert('hiii')
    //get the ping title and ping icon
    var data = {
        pingTitle: icon.charAt(0).toUpperCase() + icon.slice(1).toLowerCase(),
        pingIcon: "/static/images/bigicons/" + icon + ".png"
    };
    return Mustache.render('{{=[[ ]]=}}' + $('#epleaf-tmpl').html(), data);
}

function pleaf(pid) {
    var dat = {
        pingTitle: "",
        pingIcon: "",
        pingDesc: "",
        pingTime: "",
        pingLocation: ""
    };
    $.ajax({
        url: '/api/v1/ping/' + pid,
        type: 'GET',
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        success: function (data, status) {
            dat.pingLocation = data.city + ", " + data.state + ", " + data.country
            dat.pingTitle = data.pc;
            dat.pingDesc = data.pd;
            dat.pingTime = data.timezone, console.log(data);
        },
        async: false
    });
    var template = $("#pleaf-tmpl").html();
    return Mustache.render('{{=[[ ]]=}}' + template, dat);
}

function duration_handler(dura) {
    if (valid_durations.indexOf(dura) >= 0) {
        switch (dura) {
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

function showError(er) {
    // try to get the data from backend via ip address
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status == 200) {
            var obj = JSON.parse(this.responseText);
            lat = obj.data.lat;
            lng = obj.data.lng;
        }
    };
    xhttp.open('GET', '/api/v1/user/geoinfo?t=' + Math.random(), true)
    xhttp.send();
}

function chkLatLng() {
    if (lat != undefined && lng != undefined) {
        clearInterval(interval)
        initMap();
    }
}

function loadLayer() {
    var bnds = map.getBounds();
    var swlatlon = bnds.getSouthWest().wrap();
    var nelatlon = bnds.getNorthEast().wrap();
    var bbox = [swlatlon.lat, swlatlon.lng, nelatlon.lat, nelatlon.lng];
    url = '/api/v1/pings/map/' + duration + '/' + bbox[0] + '/' + bbox[1] + '/' + bbox[2] + '/' + bbox[3];
    map.eachLayer(function (layer) {
        if (default_mbox_layers.indexOf(layer._leaflet_id) < 0) {
            map.removeLayer(layer);
        }
    });
    var featureLayer = L.mapbox.featureLayer().loadURL(url).on('ready', function (e) {
        try {
            //update the footer with ping count
            document.getElementById('ftr').innerHTML = "Total Pings Loaded: " + Object.keys(e.target._layers).length;
            var clusterGroup = new L.MarkerClusterGroup({
                showCoverageOnHover: false,
                animateAddingMarkers: true
            });
            clusterGroup.on('click', function (a) {
                console.log('marker' + a.layer.feature.properties.id);
            });
            clusterGroup.on('clusterclick', function (a) {
                console.log('marker' + a.layer.getAllChildMarkers().length);
            });
            e.target.eachLayer(function (layer) {
                layer.setIcon(new L.icon({
                    iconUrl: layer.feature.properties.iconUrl
                }));
                layer.on('click', function (e) {
                    //e.bindPopup("hello");
                    layer.bindPopup(pleaf(layer.feature.properties.id));
                });
                clusterGroup.addLayer(layer);
            });
            map.addLayer(clusterGroup);
        } catch (err) {
            console.log("Got invalid geojson object");
        }
    });
}

function initMap() {
    L.mapbox.accessToken = 'pk.eyJ1IjoiYmFydHBpbmciLCJhIjoiY2pheTQ4Nm85MTQzejJxcWs4dWc5Yzh3cyJ9.0UyqGF5cYVmWnWmVodADYQ';
    map = L.mapbox.map('map', 'mapbox.dark', {
        minZoom: 4,
        zoomControl: false,
        worldCopyJump: true,
        attributionControl: false,
        dragRotate: false
    }).setView([lat, lng], 8);
    map.eachLayer(function (lyer) {
        default_mbox_layers.push(lyer._leaflet_id);
    });
    loadLayer();
    map.on('zoomend', function (e) {
        loadLayer();
    });
    map.on('moveend', function (e) {
        loadLayer();
    });
}
$(function () {
    $("#clickclose").click(function () {
        alert("The paragraph was clicked.");
    });
});

function clickfunction() {
    alert("I am an alert box!");
}
