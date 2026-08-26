# Field values for mygramclient.pc.
#
# The file is relocatable: pkg-config expands ${pcfiledir} to the directory the
# .pc was found in, so a prefix installed anywhere still resolves. install()
# writes the file at <prefix>/<libdir>/pkgconfig, which means the number of
# parent steps back to the prefix follows CMAKE_INSTALL_LIBDIR and is three on
# a multiarch layout such as lib/x86_64-linux-gnu.

#[[
Compute the prefix, libdir, includedir and C++ runtime flag mygramclient.pc
needs for one pair of GNUInstallDirs values.

An absolute libdir or includedir is emitted verbatim, since nothing relates it
to the prefix. An absolute libdir also makes the .pc location independent of
the prefix, so the relocatable form cannot be used for it.
]]
function(mygramdb_client_pkgconfig_fields libdir includedir out_prefix out_libdir out_includedir out_cxx_runtime)
  if(IS_ABSOLUTE "${libdir}")
    set(prefix "${CMAKE_INSTALL_PREFIX}")
  else()
    set(prefix "\${pcfiledir}")
    string(REPLACE "/" ";" pkgconfig_components "${libdir}/pkgconfig")
    foreach(component IN LISTS pkgconfig_components)
      string(APPEND prefix "/..")
    endforeach()
  endif()

  if(IS_ABSOLUTE "${libdir}")
    set(resolved_libdir "${libdir}")
  else()
    set(resolved_libdir "\${prefix}/${libdir}")
  endif()

  if(IS_ABSOLUTE "${includedir}")
    set(resolved_includedir "${includedir}")
  else()
    set(resolved_includedir "\${prefix}/${includedir}")
  endif()

  # A pure-C consumer linking with `pkg-config --static` has to resolve the C++
  # runtime the archive was built against; only a C++ link adds it implicitly.
  set(cxx_runtime "-lstdc++")
  foreach(implicit_library IN LISTS CMAKE_CXX_IMPLICIT_LINK_LIBRARIES)
    if(implicit_library STREQUAL "c++" OR implicit_library STREQUAL "stdc++")
      set(cxx_runtime "-l${implicit_library}")
      break()
    endif()
  endforeach()

  set(${out_prefix} "${prefix}" PARENT_SCOPE)
  set(${out_libdir} "${resolved_libdir}" PARENT_SCOPE)
  set(${out_includedir} "${resolved_includedir}" PARENT_SCOPE)
  set(${out_cxx_runtime} "${cxx_runtime}" PARENT_SCOPE)
endfunction()
