%{!?mygramdb_version: %global mygramdb_version 0.0.0}

Name:           mygramdb
Version:        %{mygramdb_version}
Release:        1%{?dist}
Summary:        C++ in-memory full-text search engine with MySQL replication

License:        MIT
URL:            https://github.com/libraz/mygram-db
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  gcc-c++ >= 9
BuildRequires:  cmake >= 3.15
BuildRequires:  mysql-community-devel >= 8.0
BuildRequires:  libicu-devel
BuildRequires:  readline-devel
BuildRequires:  pkgconfig

Requires:       mysql-community-libs >= 8.0
Requires:       libicu
Requires:       readline

%description
MygramDB is a high-performance C++ in-memory full-text search engine designed
for MySQL replication with GTID-based binlog tracking. It provides efficient
n-gram based indexing and search capabilities.

Features:
- In-memory n-gram full-text search
- MySQL replication (GTID-based binlog tracking)
- TCP/HTTP protocol support
- Snapshot persistence
- High-performance roaring bitmap indexes

%prep
%autosetup

%build
export MYGRAMDB_VERSION=%{mygramdb_version}
%cmake -DCMAKE_BUILD_TYPE=Release \
       -DBUILD_TESTS=OFF \
       -DUSE_ICU=ON \
       -DUSE_MYSQL=ON \
       -DBUILD_SHARED_LIBS=OFF
%cmake_build

%install
%cmake_install

# Remove unnecessary files (static build, so no shared libraries needed)
rm -rf %{buildroot}/usr/etc
rm -rf %{buildroot}/usr/share/mygramdb
rm -rf %{buildroot}/usr/share/doc/mygramdb
rm -rf %{buildroot}/usr/include
rm -rf %{buildroot}/usr/lib64/cmake
rm -rf %{buildroot}/usr/lib64/pkgconfig
find %{buildroot}/usr/lib64 -name '*.a' -delete 2>/dev/null || true
find %{buildroot}/usr/lib64 -name '*.so*' -delete 2>/dev/null || true
find %{buildroot}/usr/lib -name 'libmygramclient.a' -delete 2>/dev/null || true
find %{buildroot}/usr/lib -name 'libmygramclient.so*' -delete 2>/dev/null || true

# Create config directory
install -d %{buildroot}%{_sysconfdir}/%{name}
install -m 644 examples/config-minimal.yaml %{buildroot}%{_sysconfdir}/%{name}/config.yaml.example

# Create systemd service file
install -d %{buildroot}/usr/lib/systemd/system
cat > %{buildroot}/usr/lib/systemd/system/%{name}.service << EOF
[Unit]
Description=MygramDB Full-Text Search Engine
After=network.target mysql.service
Wants=mysql.service

[Service]
Type=simple
User=mygramdb
Group=mygramdb
ExecStart=%{_bindir}/mygramdb -c %{_sysconfdir}/mygramdb/config.yaml
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=mygramdb

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/mygramdb

[Install]
WantedBy=multi-user.target
EOF

# Create data directory
install -d %{buildroot}%{_sharedstatedir}/%{name}

%pre
getent group mygramdb >/dev/null || groupadd -r mygramdb
getent passwd mygramdb >/dev/null || \
    useradd -r -g mygramdb -d %{_sharedstatedir}/%{name} -s /sbin/nologin \
    -c "MygramDB service user" mygramdb
exit 0

%post
%systemd_post %{name}.service
if [ -d %{_sharedstatedir}/%{name} ]; then
    chown -R mygramdb:mygramdb %{_sharedstatedir}/%{name}
fi

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun_with_restart %{name}.service

%files
%license LICENSE
%doc README.md
%{_bindir}/mygramdb
%{_bindir}/mygram-cli
/usr/lib/systemd/system/%{name}.service
%dir %{_sysconfdir}/%{name}
%config(noreplace) %{_sysconfdir}/%{name}/config.yaml.example
%dir %attr(0755,mygramdb,mygramdb) %{_sharedstatedir}/%{name}

# Static build - no devel package needed

%changelog
* Tue Nov 19 2025 libraz <libraz@libraz.net> - 1.2.0-1
- Network ACL deny-by-default security enhancement
- Configuration hot reload via SIGHUP
- MySQL failover detection with server UUID tracking
- Rate limiting and connection limits
- See CHANGELOG.md for full details

* Sun Nov 17 2025 libraz <libraz@libraz.net> - 1.1.0-1
- Query result caching with n-gram invalidation
- Network ACL with CIDR filtering
- Prometheus metrics endpoint
- MySQL SSL/TLS support
- See docs/releases/v1.1.0.md for details

* Wed Nov 13 2025 libraz <libraz@libraz.net> - 1.0.0-1
- Initial RPM release
