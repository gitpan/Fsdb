Summary: A set of commands for manipulating flat-text databases from the shell
Name: perl-Fsdb
Version: 2.28
Release: 1
License: GPLv2
Group: Development/Libraries
URL: http://www.isi.edu/~johnh/SOFTWARE/FSDB/
Source0: http://www.isi.edu/~johnh/SOFTWARE/FSDB/Fsdb-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch
BuildRequires:  perl-Test-Simple
Requires:       perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))
# next line for rpmlint perl-Fsdb.noarch: W: obsolete-not-provided perl-Jdb
Obsoletes:   perl-Jdb < 2.12
Provides:  perl-Jdb = 2.12

BuildRequires:  perl(ExtUtils::MakeMaker)



%description
FSDB is package of commands for manipulating flat-ASCII databases from
shell scripts.  FSDB is useful to process medium amounts of data (with
very little data you'd do it by hand, with megabytes you might want a
real database).  FSDB is very good at doing things like:

	- extracting measurements from experimental output
	- re-examining data to address different hypotheses
        - joining data from different experiments
	- eliminating/detecting outliers
	- computing statistics on data (mean, confidence intervals,
		correlations, histograms)
	- reformatting data for graphing programs

Rather than hand-code scripts to do each special case, FSDB provides
higher-level functions.  Although it's often easy throw together a
custom script to do any single task, I believe that there are several
advantages to using this library: it is higher-level than raw perl,
control uses names instead of column numbers, it is self-documenting,
and it is very robust (error cases, careful memory handling, etc.).

%prep
%setup -q -n Fsdb-%{version}

%build
%{__perl} Makefile.PL INSTALLDIRS=vendor OPTIMIZE="$RPM_OPT_FLAGS"
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make pure_install PERL_INSTALL_ROOT=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT -type f -name .packlist -exec rm -f {} ';'
find $RPM_BUILD_ROOT -type f -name '*.bs' -empty -exec rm -f {} ';'
find $RPM_BUILD_ROOT -type d -depth -exec rmdir {} 2>/dev/null ';'
chmod -R u+w $RPM_BUILD_ROOT/*

%check
make test

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%doc README
%{_bindir}/*
%{perl_vendorlib}/Fsdb.pm
%{perl_vendorlib}/Fsdb/
%{_mandir}/man1/*.1*
%{_mandir}/man3/*.3pm*


%changelog
* Thu Nov 15 2012 johnh - 2.28-1
- See http://www.isi.edu/~johnh/SOFTWARE/FSDB/
