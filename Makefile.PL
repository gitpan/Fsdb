
#
# Makefile.PL
# $Id: 74e29a26589ae1dceb822b593f6e7ac79c32273c $
#
# made by module-starter,  8-Nov-07
#

use strict;
use warnings;
use ExtUtils::MakeMaker;

# BEGIN MODULE LIST
# (flag for update_modules)

# update modularized programs here and in lib/Fsdb/Filter/dbpipeline.pm
my @modularized_db_programs = qw(
	dbcol
	dbcolcopylast
	dbcolcreate
	dbcoldefine 
	dbcolhisto 
	dbcolmerge 
	dbcolmovingstats
	dbcolneaten
	dbcolpercentile
	dbcolrename
	dbcolscorrelate 
	dbcolsplittocols 
	dbcolsplittorows
	dbcolsregression
	dbcolstats
	dbcolstatscores
	dbfilealter
	dbfilepivot
	dbfilestripcomments 
	dbfilevalidate
	dbformmail 
	dbjoin
	dbmapreduce
	dbmerge
	dbmerge2
	dbmultistats 
	dbrow
	dbrowaccumulate 
	dbrowcount
	dbrowdiff 
	dbroweval
	dbrowuniq 
	dbrvstatdiff
	dbsort
);
my @modularized_db_non_programs = qw(
    dbpipeline
    dbsubprocess
);
my @modularized_db_converters = qw(
    	cgi_to_db
	combined_log_format_to_db
	csv_to_db 
	db_to_csv 
	db_to_html_table 
	html_table_to_db 
	kitrace_to_db 
	mysql_to_db 
	tabdelim_to_db 
	tcpdump_to_db
	xml_to_db
);
# helper programs: stubs that provide CLI glue around perl modules
my @helper_db_programs = qw(
	dbrowenumerate
);
# backwards programs are bin/db* programs that are just for jdb-1.x compatibility
my @backwards_db_programs = qw(
	dbcolize 
	dblistize 
	dbrecolize 
	dbstats 
	ns_to_db 
);
# discarded programs:
#	dbcoltighten 
#	dbfilesplit 
#	dbstipleadingspace
#	dbstripexraheaders
#	db2dcliff 
#	dbcolmultiscale 
#	dbrowsplituniq 
#
#	crl_to_db 
#	ipchain_logs_to_db 
#	dmalloc_to_db 

# (flag for update_modules)
# END MODULE LIST

my @programs_to_install = (@modularized_db_programs, @modularized_db_converters, @helper_db_programs, @backwards_db_programs);

require 5.008;

WriteMakefile(
    NAME                => 'Fsdb',
    AUTHOR              => 'John Heidemann <johnh@isi.edu>',
    VERSION_FROM        => 'lib/Fsdb.pm',
    ABSTRACT_FROM       => 'lib/Fsdb.pm',
    ($] >= 5.010 ?
      (LICENSE 		=> 'GPL-2',) : ()),
    EXE_FILES       => [ map "bin/$_", @programs_to_install ],
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'Text::CSV_XS' => 0,
        'HTML::Parser' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Fsdb-*' },
);

open(MF, ">>Makefile") or die "$0: cannot append to Makefile.\n";

foreach (@modularized_db_programs, @modularized_db_converters) {
    print MF "all :: bin/$_\n\n" .
	"bin/$_: lib/Fsdb/Filter/$_.pm ./programize_module\n" .
	"\t\$(PERL) ./programize_module $_ < lib/Fsdb/Filter/$_.pm > \$\@\n\n";
};

print MF <<'END';

#
# standard option propagation:
# (use in development after editing Filter.pm's common documentation)
#
standardoption:
	echo NOT YET DONE


#
# release stuff:
#

README: lib/Fsdb.pm
	pod2man $< | groff -man -P-b -Tascii > $@

README.html: lib/Fsdb.pm
	pod2html --noindex $<  > $@

all :: README README.html

SPEC_PREFIX=Fsdb
RPM_ARCH=noarch
RPM_PREFIX=perl-Fsdb

Fsdb.spec: lib/Fsdb.pm
	overwrite $@ sed "s/^Version: .*$$/Version: $(VERSION)/"

rpms: $(SPEC_PREFIX).spec Fsdb.spec
	cp $(SPEC_PREFIX)-$(VERSION).tar.gz $$HOME/rpmbuild/SOURCES
	cp $(SPEC_PREFIX).spec $$HOME/rpmbuild/SPECS
	( cd $$HOME/rpmbuild; rpmbuild --sign -ba SPECS/$(SPEC_PREFIX).spec; )
	cp $$HOME/rpmbuild/RPMS/$(RPM_ARCH)/$(RPM_PREFIX)-$(VERSION)-1.$(RPM_ARCH).rpm .
	cp $$HOME/rpmbuild/SRPMS/$(RPM_PREFIX)-$(VERSION)-1.src.rpm .

signrpms: 
	rpmsign --addsign $(RPM_PREFIX)-$(VERSION)*.rpm

WEBDIR=/home/johnh/WORKING/JOHNH_WWW/SOFTWARE/FSDB

release:
	@echo "You should have done (fix date in Fsdb.pm); \"make manifest; make dist; make rpms\""
	-make signrpms
	cp $(RPM_PREFIX)-$(VERSION)-1.$(RPM_ARCH).rpm $(WEBDIR)
	cp $(RPM_PREFIX)-$(VERSION)-1.src.rpm $(WEBDIR)
	cp $(SPEC_PREFIX)-$(VERSION).tar.gz $(WEBDIR)
	cp README.html $(WEBDIR)/$(SPEC_PREFIX)-$(VERSION)_README.html
	cd $(WEBDIR); git add $(RPM_PREFIX)-$(VERSION)*.rpm $(SPEC_PREFIX)-$(VERSION).tar.gz $(SPEC_PREFIX)-$(VERSION)_README.html
	cd $(WEBDIR); rm Fsdb-current.tar.gz; ln -s Fsdb-$(VERSION).tar.gz Fsdb-current.tar.gz; rm perl-Fsdb-current.noarch.rpm; ln -s perl-Fsdb-$(VERSION)-1.noarch.rpm perl-Fsdb-current.noarch.rpm; rm perl-Fsdb-current.src.rpm; ln -s perl-Fsdb-$(VERSION)-1.src.rpm perl-Fsdb-current.src.rpm; 
	@echo "to finish: git commit -am $(VERSION)-release; git tag -s -u johnh@isi.edu -m $(VERSION)-release fsdb-$(VERSION); cd $(WEBDIR); git commit -am \"release $(RPM_PREFIX)-$(VERSION)\""

END
close (MF);
