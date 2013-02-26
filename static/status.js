//
//
CrawlJob = function(el) {
  this.el = jQuery(el);
  this.el.obj = this;
  this.name = this.el.attr('jobname');
  this.last_status = {};

  var px = jQuery.proxy;
  jQuery('.seencheck', this.el).submit(px(this, 'seencheck'));
  jQuery('.showstatus', this.el).click(px(this, 'show_status'));
  jQuery('button.update-seencount', this.el).click(px(this, 'update_seencount'));
  jQuery('button.flush', this.el).click(px(this, 'flush_outq'));
  jQuery('button.clear-seen', this.el).click(px(this, 'clear_seen'));
  jQuery('.jtitle', this.el).click(px(this, 'toggle'));
};
CrawlJob.prototype = {
  name: null,
  el: null, // jQuery object
  toggle:function() {
    this.el.toggleClass('closed');
  },
  clear_seen:function(ev) {
    if (!confirm('are you sure you want to wipe out'+
		 ' seen list for job "' + this.name + '"?'))
      return;
    jQuery.ajax('jobs/'+this.name+'/clearseen', {
      dataType:'json',
      beforeSend:function(){ jQuery(ev.target).addClass('wait') },
      complete:function(){ jQuery(ev.target).removeClass('wait') }
    });
    return false;
  },
  flush_outq:function(ev) {
    var btn = ev.target;
    jQuery.ajax('jobs/'+encodeURIComponent(this.name)+'/flush', {
      dataType:'json',
      beforeSend:function(){ jQuery(btn).addClass('wait'); },
      success:function(data) {
	if (!data.success) {
	  alert('job "' + this.name + '" flush failed');
	}
      },
      complete:function(){ jQuery(btn).removeClass('wait'); }
    });
    return false;
  },
  update_seencount:function() {
    var showel = jQuery('.seencount', this.el);
    showel.addClass('loading wait');
    jQuery.ajax('jobs/'+encodeURIComponent(this.name)+'/seencount', {
      dataType:'json',
      success:function(data) {
	if (typeof(data.seencount) == 'undefined') {
	  showel.text('-?-');
	} else {
	  showel.text(with_comma(data.seencount));
	}
      },
      complete:function() { showel.removeClass('loading wait'); }
    });
  },
  show_status:function(ev) {
    jQuery.ajax('q/statuses', {
      dataType:'json',
      data:{ job:this.name },
      success:jQuery.proxy(this, 'render_statuses'),
      beforeSend:function() { jQuery(ev.target).addClass('wait'); },
      complete:function() { jQuery(ev.target).removeClass('wait'); }
    });
    return false;
  },
  render_statuses:function(resp) {
    if (!resp.success) {
      return;
    }
    var servers = resp.servers;
    var e = jQuery('.jobstatus', this.el);
    e.empty();
    for (svid in servers) {
      var server = servers[svid];
      server.svid = svid;
      var ee = jQuery('<fieldset>').attr({svid:svid, server:server.name})
	.appendTo(e);
      ee.append(jQuery('<legend>').text(svid+':'+server.name));
      this.render_status(ee, server);
    }
  },
  render_status:function(d, resp) {
    if (!resp.success) return;
    var data = resp.r;
    if ('workqueuesize' in data) {
      jQuery('<div>').text('WorkQueueSize: ' + data.workqueuesize).appendTo(d);
    }
    var last_status_update = 0, last_inq_in, last_inq_out;
    var last = this.last_status[resp.name];
    if (last) {
      last_status_update = last.update;
      last_inq_in = last.inq_in;
      last_inq_out = last.inq_out;
    }
    var inq = data.inq;
    if (inq) {
      var inqdiv = jQuery('<div>');
      var in_text = 'in=' + with_comma(inq.addedcount);
      var dequeuecount, qfile_text = '-';
      if (inq.dequeue) {
	dequeuecount = inq.dequeue.dequeuecount || 0;
	qfile_text = 'qfile=' + (inq.dequeue.qfile || '(none)') + ', '
	  + (inq.dequeue.qfilestep || '-');
      }
      var out_text = 'out=' + with_comma(dequeuecount);
      var bf_text = 'buffered=' + with_comma(inq.bufferedcount || 0);
      var nq_text = 'nqfiles=' + with_comma(inq.queuefilecount);
      var now = (new Date()).getTime();
      var elapsed_ms = now - last_status_update;
      if (elapsed_ms < 3600000) {
	var in_speed = (inq.addedcount - last_inq_in) / (elapsed_ms / 1000);
	if (in_speed >= 0 && in_speed < 1000000) {
	  in_text += ' (' + (Math.floor(in_speed * 10)/10) + ' URI/s)';
	}
	var out_speed = (dequeuecount - last_inq_out) / (elapsed_ms / 1000);
	if (out_speed >= 0 && out_speed < 100000) {
	  out_text += ' (' + (Math.floor(out_speed * 10)/10) + ' URI/s)';
	}
      }
      inqdiv.append('IncomingQueue: ' + in_text +
		    ' | ' + bf_text +
		    ' | ' + nq_text + ', ' + qfile_text +
		    ' | ' + out_text
		   );
      this.last_status[resp.name] = {
	update: now, inq_in: inq.addedcount, inq_out: dequeuecount
      };
      d.append(inqdiv);
    }
    var sch = data.sch;
    if (sch) {
      if (sch.clients) {
	var clients = [];
	jQuery.each(sch.clients, function(k, v) { v.id = k; clients.push(v); });
	clients.sort(function(a, b){ return a.id - b.id; });
	d.append(jQuery('<div>').html('Clients:'));
	var tbl = jQuery('<table border="1">');
	var r = tbl.get(0).insertRow(-1);
	jQuery.each(['id','scheduled','fed','finished','next','worksets',
		     'lastfed','lastfed_t','nqfiles'],
	       function(i, s){ jQuery(r.insertCell(-1)).text(s); });
	jQuery.each(clients, function(i, v){
	    var k = v.id;
	    r = tbl.get(0).insertRow(-1);
	    jQuery(r).addClass('client');
	    if (v.isactive === true) {
	      jQuery(r).addClass('active');
	    } else if (v.isactive === false) {
	      jQuery(r).addClass('inactive');
	    }
	    if (v.worksetcount === 0) {
	      jQuery(r).addClass('has-problem');
	    }
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
  },
  seencheck:function(ev) {
    var form = ev.target;
    var url = form.url.value;
    if (url == "") return false;
    if (!url.match('http://'))
      url = 'http://' + url;
    jQuery('#seencheckresult_'+this.name).empty();
    jQuery.ajax('jobs/'+encodeURIComponent(job)+'/seen', {
      data:{ u:url },
      dataType:'json',
      success:function(data) {
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
  },
  schedule:function(ev) {
    var form = ev.target;
    var url = form.u;
    if (url) {
      form.disabled = true;
      jQuery.ajax('jobs/'+this.name+'/discovered', {
	type:'POST',
	data:{u:url, force:1},
	dataType:'json',
	success:function(data, status, xhr) {
	  jQuery('#seencheckresult_'+job).html(data.scheduled ? 'scheduled'
	    : 'already seen');
	},
	complete:function() {
	  form.disabled = false;
	}
      });
    }
  }
};
function with_comma(n) {
  var t = typeof(n);
  if (t == "undefined" || n == null) {
    return "-?-";
  } else if (t == "string") {
    return n;
  } else if (t == "number") {
    var digits = String(n).match(/./g);
    for (var i = digits.length - 3; i > 0; i -= 3) {
      digits.splice(i, 0, ',');
    }
    return digits.join('');
  } else {
    return n.toString();
  }
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
jQuery('.job').each(function(i, el) {
  new CrawlJob(el);
});
