function schedule(ev) {
  var url = this.u;
  if (url) {
    var b = ev.target;
    var job = getjobname(b);
    if (!job) { alert('failed to identify job'); return; }
    b.disabled = true;
    jQuery.ajax('jobs/'+job+'/discovered', {
      type:'POST',
      data:{u:url, force:1},
      dataType:'json',
      success:function(data, status, xhr) {
        jQuery('#seencheckresult_'+job).html(data.scheduled ? 'scheduled'
          : 'already seen');
      },
    });
  }
}
function seencheck(ev) {
  var job = getjobname(this);
  //console.log(this);
  var url = this.url.value;
  if (url == "") return false;
  if (!url.match('http://'))
    url = 'http://' + url;
  //console.log(url);
  jQuery('#seencheckresult_'+job).empty();
  jQuery.ajax('jobs/'+encodeURIComponent(job)+'/seen', {
    data:{u:url},
    dataType:'json',
    success:function(data, status, xhr) {
      //console.log(data);
      var tbl = [];
      var seen = 0;
      var u = data.u;
      if (u) {
        if (u.id) { tbl.push(['ID', u.id]); }
        if (u.f) {
          tbl.push(['Crawled on', new Date(u.f*1000).toString()]);
          seen = 1;
        }
        if (u.e) {
          tbl.push(['Expires on', new Date(u.e*1000).toString()]);
          seen = 1;
        }
      }
      var e = jQuery('#seencheckresult_'+job).empty();
      e.append(seen ? 'seen' : 'not seen');
      jQuery('<button>Schedule</button>')
        .click(jQuery.proxy(schedule, {u:url})).appendTo(e);
      if (tbl.length > 0) {
        var t = jQuery('<table border="1">').get(0);
        jQuery.each(tbl, function(i, r){
            jQuery('<tr>').append(
              jQuery('<td>').append(r[0]), jQuery('<td>').append(r[1]))
	      .appendTo(t);
	  });
	e.append(t);
      }
    }
  });
  return false;
}
var last_status_update = 0;
var last_inq_in = 0;
var last_inq_out = 0;
function renderstatus(resp, status, xhr) {
  if (!resp.success) {
    return;
  }
  var data = resp.r;
  //console.log(data);
  var d = jQuery('.jobstatus', '.job[jobname="'+data.job+'"]');
  d.empty();
  if ('workqueuesize' in data) {
    jQuery('<div>').text('WorkQueueSize: ' + data.workqueuesize).appendTo(d);
  }
  var inq = data.inq;
  if (inq) {
    var inqdiv = jQuery('<div>');
    var in_text = 'in=' + with_comma(inq.addedcount);
    var out_text = 'out=' + with_comma(inq.processedcount);
    var nq_text = 'nqfiles=' + with_comma(inq.queuefilecount);
    var now = (new Date()).getTime();
    var elapsed_ms = now - last_status_update;
    if (elapsed_ms < 3600000) {
      var in_speed = (inq.addedcount - last_inq_in) / (elapsed_ms / 1000);
      if (in_speed >= 0 && in_speed < 1000000) {
        in_text += ' (' + (Math.floor(in_speed * 10)/10) + ' URI/s)';
      }
      var out_speed = (inq.processedcount - last_inq_out) / (elapsed_ms / 1000);
      if (out_speed >= 0 && out_speed < 100000) {
        out_text += ' (' + (Math.floor(out_speed * 10)/10) + ' URI/s)';
      }
    }
    inqdiv.append('IncomingQueue: ' + in_text + ', ' + out_text +
		  ', ' + nq_text);
    last_status_update = now;
    last_inq_in = inq.addedcount;
    last_inq_out = inq.processedcount;
    d.append(inqdiv);
  }
  var sch = data.sch;
  if (sch) {
    var clients = sch.clients;
    if (clients) {
      d.append(jQuery('<div>').html('Clients:'));
      var tbl = jQuery('<table border="1">');
      var r = tbl.get(0).insertRow(-1);
      jQuery.each(['id','scheduled','fed','finished','next','worksets',
		   'lastfed','lastfed_t','nqfiles'],
             function(i, s){ jQuery(r.insertCell(-1)).text(s); });
      jQuery.each(clients, function(k, v){
          r = tbl.get(0).insertRow(-1);
          jQuery(r).addClass('client');
          jQuery(r.insertCell(-1)).text(k).attr({align:'right'});
          jQuery.each(['scheduledcount','feedcount','finishedcount','next','worksetcount','lastfeedcount','lastfeedtime','qfilecount'],
		function(i, p){
		    if (v[p] == null)
		        v[p] = '-';
		    else if (p.match('count$'))
			v[p] = with_comma(v[p]);
		    else if (p.match('time$'))
		        v[p] = timedelta(v[p]);
		    jQuery(r.insertCell(-1)).html(v[p]).attr({align:'right'});
		});
        });
      d.append(tbl);
    }
  }
}
function getjobname(e) {
  return jQuery(e).closest('.job').attr('jobname');
}
function showstatus(ev) {
  ev.preventDefault();
  var job = getjobname(this);
  if (!job) { alert('failed to identify job name'); return; }
  jQuery.ajax('jobs/'+job+'/status', {
    dataType:'json',
    success: renderstatus,
    beforeSend: function(){ jQuery(ev.target).addClass('wait'); },
    complete: function() { jQuery(ev.target).removeClass('wait'); }
  });
}
function with_comma(n) {
  var digits = String(n).match(/./g);
  for (var i = digits.length - 3; i > 0; i -= 3) {
    digits.splice(i, 0, ',');
  }
  return digits.join('');
}
function timedelta(t) {
  if (t == null || typeof(t) == 'undefined') return '-';
  var d = Math.floor(t * 1000 - Date.now());
  var s = '';
  if (d < 0) {
    d = Math.abs(d);
    s = '-';
  }
  var ms = d % 1000;
  d = Math.floor(d / 1000);
  var r = (d % 60)+'<span class="tsp">s</span>'+ms+'<span class="tsp">ms</span>';
  d = Math.floor(d / 60);
  if (d > 0) {
    r = ((d % 60) + '<span class="tsp">m</span>') + r;
    d = Math.floor(d / 60);
    if (d > 0) {
      r = d + '<span class="tsp">h</span>' + r;
    }
  }
  return s+r;
}
function update_seencount() {
  var job = getjobname(this);
  var jobel = jQuery(this).closest('.job');
  jQuery('.seencount', jobel).addClass('loading wait');
  jQuery.ajax('jobs/'+encodeURIComponent(job)+'/seencount', {
    dataType:'json',
    success: function(data){
      jQuery('.seencount', jobel).text(with_comma(data.seencount));
    },
    complete: function(){ jQuery('.seencount', jobel).removeClass('loading wait'); }
  });
}
function flush_job() {
  var btn = this;
  var job = getjobname(this);
  jQuery.ajax('jobs/'+encodeURIComponent(job)+'/flush', {
    dataType:'json',
    beforeSend:function(){jQuery(btn).addClass('wait');},
    success:function(data){
      if (!data.ok) {
        alert('job ' + job + ' flush failed');
      }
    },
    complete:function(){jQuery(btn).removeClass('wait');}
  });
}
function clear_seen(ev) {
    var job = jQuery(this).attr('jobname');
    if (!confirm('are you sure you want to wipe out'+
		 ' seen list for job "'+job+'"?'))
	return;
    jQuery.ajax('jobs/'+job+'/clearseen', {
	    dataType:'json',
		beforeSend:function(){jQuery(ev.target).addClass('wait')},
		complete:function(){jQuery(ev.target).removeClass('wait')}
	});
}

jQuery('.seencheck').submit(seencheck);
jQuery('.showstatus').click(showstatus);
jQuery('button.update-seencount').click(update_seencount);
jQuery('button.flush').click(flush_job);
jQuery('.job').each(function(i, job){
	jQuery('button.clear-seen', job).click(jQuery.proxy(clear_seen, job));
    });
