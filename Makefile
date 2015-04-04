#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2015, Joyent, Inc.
#

ROOT = $(PWD)
JS_CHECK_TARGETS = report.js
JSLINT = $(ROOT)/tools/javascriptlint/build/install/jsl
JSSTYLE = $(ROOT)/tools/jsstyle
JSSTYLE_OPTS = -o indent=4,strict-indent=1,doxygen,unparenthesized-return=0,continuation-at-front=1,leading-right-paren-ok=1
NATIVE_CC = /opt/local/bin/gcc

# On Darwin/OS X we support running 'make check'
ifeq ($(shell uname -s),Darwin)
PATH = /bin:/usr/bin:/usr/sbin:/sbin:/opt/local/bin
else
PATH = /usr/bin:/usr/sbin:/sbin:/opt/local/bin
endif

jsl: $(JSLINT)

$(JSLINT):
	(cd $(ROOT)/tools/javascriptlint; $(MAKE) install)

check: $(JSLINT)
	@$(JSLINT) --nologo --conf=$(ROOT)/tools/jsl.node.conf \
		$(JS_CHECK_TARGETS)
	@(for file in $(JS_CHECK_TARGETS); do \
		echo $(PWD)/$$file; \
		$(JSSTYLE) $(JSSTYLE_OPTS) $$file; \
		[[ $$? == "0" ]] || exit 1; \
	done)
