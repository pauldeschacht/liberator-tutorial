Quality Monitor
===============


Quality Monitor implemented as REST web service. 

The web service is based on [liberator](clojure-liberator.github.io/liberator). External scripts upload metrics to the web service (such as cpu usage, file size). The format for the metric is inspired by [riemann.io](http://riemann.io) and is done as JSON over HTTP.

External and internals scripts retrieve the metrics and will calculate the quality of these metrics. The quality is uploaded again to the web service and can eventually trigger an alert (if quality is below is given threshold)



