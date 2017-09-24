
-- define target: libxredis.a
target("xredis")

    -- set kind
    set_kind("static")

    -- add files
    add_files("*.cpp")

    -- add headers
    add_headers("*.h")
    set_headerdir("$(buildir)/xredis")

    -- add links
    add_links("pthread")

    -- add defines
--    add_defines("USE_JEMALLOC")

    -- add other links
--    add_links("jemalloc", "tcmalloc")

-- define target: xredis_server
target("xredis_server")

    -- set kind
    set_kind("binary")

    -- add deps: libxredis.a
    add_deps("xredis")

    -- add files
    add_files("*.cpp")


