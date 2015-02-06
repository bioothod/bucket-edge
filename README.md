# bucket-edge

Whole bucket defragmenatation tool for backrunner (http://doc.reverbrain.com/backrunner:backrunner) proxy.
While one can use Elliptics (http://reverbrain.com/elliptics) ioclient tool for per-group defragmentation/sorting,
it is not convenient for higher levels.

Bucket-edge allows to sort/defragemnt the whole bucket by only having its name. Tool will read bucket metadata
and start defragmentation/sorting in all groups in sequence. It doesn't allow to run more than 3 defragmentation
process per server as well as running defrag on different servers in parallel. This was made deliberately
to prevent cases when all nodes (address+backend) within small bucket are being defragmented, which heavily
decreases bucket IO performance.

Bucket-edge uses the same config structure as backrunner proxy, in particular, it uses 'elliptics' section.
More information about options can be found on backrunner documentation page:
http://doc.reverbrain.com/backrunner:backrunner
