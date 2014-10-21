Summary: A set of commands for manipulating flat-text databases from the shell
Name: perl-Fsdb
Version: 2.48
Release: 1%{?dist}
License: GPLv2
Group: Development/Libraries
URL: http://www.isi.edu/~johnh/SOFTWARE/FSDB/
Source0: http://www.isi.edu/~johnh/SOFTWARE/FSDB/Fsdb-%{version}.tar.gz
# buildroot deprecated before 2013-07-26, but left in for EPEL 5
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch
BuildRequires: perl
BuildRequires: perl(Carp)
BuildRequires: perl(Config)
BuildRequires: perl(Exporter)
BuildRequires: perl(ExtUtils::MakeMaker)
BuildRequires: perl(File::Copy)
BuildRequires: perl(Getopt::Long)
BuildRequires: perl(IO::File)
BuildRequires: perl(IO::Handle)
BuildRequires: perl(IO::Uncompress::AnyUncompress)
BuildRequires: perl(Pod::Usage)
BuildRequires: perl(strict)
BuildRequires: perl(Test::More)
BuildRequires: perl(vars)
# following BRs are maybe not required?
BuildRequires:  perl(HTML::Parser)
BuildRequires:  perl(Text::CSV_XS)
BuildRequires:  perl(IO::Compress::Bzip2)
BuildRequires:  perl(IO::Compress::Gzip)
BuildRequires:  perl(IO::Compress::Xz)
Requires:       perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))
Requires:       perl(HTML::Parser)
Requires:       perl(Test::More)
Requires:       perl(Text::CSV_XS)
Requires:       perl(IO::Compress::Bzip2)
Requires:       perl(IO::Compress::Gzip)
Requires:       perl(IO::Compress::Xz)



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
%{__perl} Makefile.PL INSTALLDIRS=vendor
make %{?_smp_mflags}

%install
# buildroot removal left in for EPEL 5
rm -rf $RPM_BUILD_ROOT
make pure_install DESTDIR=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT -type f -name .packlist -exec rm -f {} ';'
# fix up g+s getting set on directories, and executables being 0555
# (*I* think those are ok, but not rpmlint.)
# find $RPM_BUILD_ROOT -type d -exec chmod g-s {} ';'
# find $RPM_BUILD_ROOT -executable -exec chmod 0755 {} ';'
%{_fixperms} %{buildroot}/*


%check
make test

# %clean but left in for EPEL 5
%clean
rm -rf $RPM_BUILD_ROOT


%files
# next line deprecated since rpm 4.4, I'm told.
# -was-percent-defattr(-,root,root,-)
# -was-percent-doc COPYING Fsdb.spec META.json MYMETA.json MYMETA.yml programize_module README README.html update_modules
%doc README COPYING
%{_bindir}/*
%{perl_vendorlib}/Fsdb.pm
%{perl_vendorlib}/Fsdb/
%{_mandir}/man1/*.1*
%{_mandir}/man3/*.3pm*


%changelog
* Mon Dec 09 2013 John Heidemann <johnh@isi.edu> 2.48-1
- See http://www.isi.edu/~johnh/SOFTWARE/FSDB/
