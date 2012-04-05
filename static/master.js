function render_jobs(data) {
    if (!data.r) return;
    var e = jQuery('<ul class="jobs">');
    jQuery.each(data.r, function(){
	    var j = jQuery('<li class="job">')
		.append(jQuery('<span>').text(this.name));
	    if (this.servers) {
		var s = jQuery('<ul class="servers">').appendTo(j);
		jQuery.each(this.servers, function(){
			jQuery('<li class="server">')
			    .text(this.name)
			    .appendTo(s);
		    });
	    }
	    e.append(j);
	});
    jQuery('#servers').empty().append(e);
}
function update_jobs() {
    jQuery.getJSON('q/jobstatus', render_jobs);
}
function render_servers(data) {
  if (!data.r) return; // TODO: report error
  var e = jQuery('<ul class="servers">');
  jQuery.each(data.r, function(){
      var s = jQuery('<li class="server">').append(
        jQuery('<span>').text(this.name),
	jQuery('<span class="vital">').text('alive' in this ? 'alive' : 'dead?'));
      if (this.jobs) {
	var j = jQuery('<ul class="jobs">').appendTo(s);
	jQuery.each(this.jobs, function(){
	    jQuery('<li class="job">').text(this.name).appendTo(j);
          });
      }
      e.append(s);
    });
  jQuery('#servers').empty().append(e);
}
function update_servers() {
  jQuery.getJSON('q/serverstatus', render_servers);
}
function update_cluster_view() {
    var vt = document.location.hash.substring(1);
    var f = {
	'': update_servers,
	'servers': update_servers,
	'jobs': update_jobs
    }[vt];
    if (!vt) return;
    f();
}
function update_urldb_count(data) {
  if (data.success) {
    jQuery('#urldb-count').text(data.r);
  }
}
