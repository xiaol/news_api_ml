+ function(d) {
	var b = function(f, g) {
		this.isCurrent = function() {
			return g == f.currentPage
		};
		this.isFirst = function() {
			return g == 1
		};
		this.isLast = function() {
			return g == f.totalPages
		};
		this.isPrev = function() {
			return g == (f.currentPage - 1)
		};
		this.isNext = function() {
			return g == (f.currentPage + 1)
		};
		this.isLeftOuter = function() {
			return g <= f.outerWindow
		};
		this.isRightOuter = function() {
			return (f.totalPages - g) < f.outerWindow
		};
		this.isInsideWindow = function() {
			if (f.currentPage < f.innerWindow + 1) {
				return g <= ((f.innerWindow * 2) + 1)
			} else {
				if (f.currentPage > (f.totalPages - f.innerWindow)) {
					return (f.totalPages - g) <= (f.innerWindow * 2)
				} else {
					return Math.abs(f.currentPage - g) <= f.innerWindow
				}
			}
		};
		this.number = function() {
			return g
		}
	};
	var e = {
		firstPage: function(i, h, g) {
			var f = d("<li>").append(d('<a href="#">').html(h.first).bind("click.bs-pagy", function() {
				i.firstPage();
				return false
			}));
			if (g.isFirst()) {
				f.addClass("disabled")
			}
			return f
		},
		prevPage: function(i, h, g) {
			var f = d("<li>").append(d('<a href="#">').attr("rel", "prev").html(h.prev).bind("click.bs-pagy", function() {
				i.prevPage();
				return false
			}));
			if (g.isFirst()) {
				f.addClass("disabled")
			}
			return f
		},
		nextPage: function(i, h, g) {
			var f = d("<li>").append(d('<a href="#">').attr("rel", "next").html(h.next).bind("click.bs-pagy", function() {
				i.nextPage();
				return false
			}));
			if (g.isLast()) {
				f.addClass("disabled")
			}
			return f
		},
		lastPage: function(i, h, g) {
			var f = d("<li>").append(d('<a href="#">').html(h.last).bind("click.bs-pagy", function() {
				i.lastPage();
				return false
			}));
			if (g.isLast()) {
				f.addClass("disabled")
			}
			return f
		},
		gap: function(g, f) {
			return d("<li>").addClass("disabled").append(d('<a href="#">').html(f.gap))
		},
		page: function(i, h, g) {
			var f = d("<li>").append(function() {
				var j = d('<a href="#">');
				if (g.isNext()) {
					j.attr("rel", "next")
				}
				if (g.isPrev()) {
					j.attr("rel", "prev")
				}
				j.html(g.number());
				j.bind("click.bs-pagy", function() {
					i.page(g.number());
					return false
				});
				return j
			});
			if (g.isCurrent()) {
				f.addClass("active")
			}
			return f
		}
	};
	var c = function(g, f) {
		this.$element = d(g);
		this.options = d.extend({}, c.DEFAULTS, f);
		this.$ul = this.$element.find("ul");
		this.render()
	};
	c.DEFAULTS = {
		currentPage: null,
		totalPages: null,
		innerWindow: 2,
		outerWindow: 0,
		first: "&laquo;",
		prev: "&lsaquo;",
		next: "&rsaquo;",
		last: "&raquo;",
		gap: "..",
		truncate: false,
		page: function(f) {
			return true
		}
	};
	c.prototype.render = function() {
		var j = this.options;
		if (!j.totalPages) {
			this.$element.hide();
			return
		} else {
			this.$element.show()
		}
		var f = new b(j, j.currentPage);
		if (!f.isFirst() || !j.truncate) {
			if (j.first) {
				this.$ul.append(e.firstPage(this, j, f))
			}
			if (j.prev) {
				this.$ul.append(e.prevPage(this, j, f))
			}
		}
		var h = false;
		for (var k = 1, l = j.totalPages; k <= l; k++) {
			var g = new b(j, k);
			if (g.isLeftOuter() || g.isRightOuter() || g.isInsideWindow()) {
				this.$ul.append(e.page(this, j, g));
				h = false
			} else {
				if (!h && j.outerWindow > 0) {
					this.$ul.append(e.gap(this, j));
					h = true
				}
			}
		}
		if (!f.isLast() || !j.truncate) {
			if (j.next) {
				this.$ul.append(e.nextPage(this, j, f))
			}
			if (j.last) {
				this.$ul.append(e.lastPage(this, j, f))
			}
		}
	};
	c.prototype.page = function(h, g) {
		var f = this.options;
		if (g === undefined) {
			g = f.totalPages
		}
		if (h > 0 && h <= g) {
			if (f.page(h)) {
				this.$ul.empty();
				f.currentPage = h;
				f.totalPages = g;
				this.render()
			}
		}
		return false
	};
	c.prototype.firstPage = function() {
		return this.page(1)
	};
	c.prototype.lastPage = function() {
		return this.page(this.options.totalPages)
	};
	c.prototype.nextPage = function() {
		return this.page(this.options.currentPage + 1)
	};
	c.prototype.prevPage = function() {
		return this.page(this.options.currentPage - 1)
	};
	var a = d.fn.pagy;
	d.fn.pagy = function(g) {
		var f = arguments;
		return this.each(function() {
			var j = d(this);
			var i = j.data("bs.pagy");
			var h = typeof g == "object" && g;
			if (!i) {
				j.data("bs.pagy", (i = new c(this, h)))
			}
			if (typeof g == "string") {
				i[g].apply(i, Array.prototype.slice.call(f, 1))
			}
		})
	};
	d.fn.pagy.Constructor = c;
	d.fn.pagy.noConflict = function() {
		d.fn.pagy = a;
		return this
	}
}(jQuery);