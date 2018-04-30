var str1 = '{"latitude":44.9723,"longitude":93.2625,"temperature":67.0,"windspeed":11.0,"visibility":1.0,"weatherAlert":"Zephyros has been kind. 2"}\n{"latitude":44.9723,"longitude":93.2625,"temperature":67.0,"windspeed":11.0,"visibility":1.0,"weatherAlert":"Zephyros has been kind. 3"}';
var arr1 = str1.split(/\r?\n/);
console.log('arr1= ', arr1);

var str2 = '{"latitude":44.9723,"longitude":93.2625,"temperature":67.0,"windspeed":11.0,"visibility":1.0,"weatherAlert":"Zephyros has been kind. 2"}';
var arr2 = str2.split(/\r?\n/);
console.log('arr2= ', arr2);

for(var i = 0; i < arr1.length; i++) {
	jsonObject = JSON.parse(arr1[i]);
	console.log("jsonObject= ", jsonObject);
	console.log("jsonObject.latitude= ", jsonObject.latitude);
	console.log("jsonObject.longitude= ", jsonObject.longitude);
	console.log("jsonObject.temperature= ", jsonObject.temperature);
	console.log("jsonObject.windspeed= ", jsonObject.windspeed);
	console.log("jsonObject.visibility= ", jsonObject.visibility);
	console.log("jsonObject.weatherAlert= ", jsonObject.weatherAlert);
	
}