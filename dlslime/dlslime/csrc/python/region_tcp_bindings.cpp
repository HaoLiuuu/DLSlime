#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../engine/tcp/tcp_region_endpoint.h"
namespace py = pybind11;
using namespace dlslime::region;
using namespace dlslime::tcp;
void bind_region_tcp(py::module_& m)
{
    py::enum_<Access>(m, "RegionAccess")
        .value("READ", Access::read)
        .value("WRITE", Access::write)
        .value("READ_WRITE", Access::read_write);
    py::class_<HostBuffer, std::shared_ptr<HostBuffer>>(m, "HostBuffer", py::buffer_protocol())
        .def(py::init<size_t>())
        .def_property_readonly("size", &HostBuffer::size)
        .def_buffer([](HostBuffer& b) {
            return py::buffer_info(b.data(), 1, py::format_descriptor<unsigned char>::format(), 1, {b.size()}, {1});
        });
    py::class_<Key>(m, "RegionKey")
        .def(py::init<>())
        .def_readwrite("scope_hi", &Key::scope_hi)
        .def_readwrite("scope_lo", &Key::scope_lo)
        .def_readwrite("id", &Key::id)
        .def_readwrite("generation", &Key::generation);
    py::class_<Descriptor>(m, "RegionDescriptor")
        .def(py::init<>())
        .def_readwrite("key", &Descriptor::key)
        .def_readwrite("length", &Descriptor::length)
        .def_readwrite("access", &Descriptor::access)
        .def("to_dict",
             [](const Descriptor& d) {
                 py::dict result;
                 result["scope_hi"]   = d.key.scope_hi;
                 result["scope_lo"]   = d.key.scope_lo;
                 result["id"]         = d.key.id;
                 result["generation"] = d.key.generation;
                 result["length"]     = d.length;
                 result["access"]     = uint32_t(d.access);
                 return result;
             })
        .def_static("from_dict", [](const py::dict& d) {
            return Descriptor{{d["scope_hi"].cast<uint64_t>(),
                               d["scope_lo"].cast<uint64_t>(),
                               d["id"].cast<uint64_t>(),
                               d["generation"].cast<uint64_t>()},
                              d["length"].cast<uint64_t>(),
                              static_cast<Access>(d["access"].cast<uint32_t>())};
        });
    py::class_<Retirement>(m, "RegionRetirement")
        .def(
            "wait", [](const Retirement& f) { f.get(); }, py::call_guard<py::gil_scoped_release>())
        .def("ready",
             [](const Retirement& f) { return f.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready; });
    py::class_<HostRegionRegistry, std::shared_ptr<HostRegionRegistry>>(m, "HostRegionRegistry")
        .def("register_buffer",
             &HostRegionRegistry::register_buffer,
             py::arg("buffer"),
             py::arg("access") = Access::read_write)
        .def("retire", &HostRegionRegistry::retire, py::call_guard<py::gil_scoped_release>())
        .def("active_leases", &HostRegionRegistry::active_leases)
        .def("close", &HostRegionRegistry::close, py::call_guard<py::gil_scoped_release>());
    py::class_<RegionPeer>(m, "TcpRegionPeer")
        .def(py::init(
                 [](std::string host, uint16_t port, uint16_t protocol) { return RegionPeer{host, port, protocol}; }),
             py::arg("host"),
             py::arg("port"),
             py::arg("protocol") = 2)
        .def_readwrite("host", &RegionPeer::host)
        .def_readwrite("port", &RegionPeer::port)
        .def_readwrite("protocol", &RegionPeer::protocol)
        .def("to_dict",
             [](const RegionPeer& p) {
                 py::dict d;
                 d["host"]     = p.host;
                 d["port"]     = p.port;
                 d["protocol"] = p.protocol;
                 return d;
             })
        .def_static("from_dict", [](const py::dict& d) {
            return RegionPeer{
                d["host"].cast<std::string>(), d["port"].cast<uint16_t>(), d["protocol"].cast<uint16_t>()};
        });
    py::class_<RegionSpan>(m, "TcpRegionSpan")
        .def(py::init([](Key local, Descriptor remote, uint64_t lo, uint64_t ro, uint64_t n) {
                 return RegionSpan{local, remote, lo, ro, n};
             }),
             py::arg("local"),
             py::arg("remote"),
             py::arg("local_offset"),
             py::arg("remote_offset"),
             py::arg("length"));
    py::class_<RegionOptions>(m, "TcpRegionOptions")
        .def(py::init<>())
        .def_readwrite("window_requests", &RegionOptions::window_requests)
        .def_readwrite("window_bytes", &RegionOptions::window_bytes)
        .def_readwrite("max_clients", &RegionOptions::max_clients)
        .def_readwrite("max_servers", &RegionOptions::max_servers)
        .def_readwrite("server_timeout_ms", &RegionOptions::server_timeout_ms);
    py::class_<RegionFuture, std::shared_ptr<RegionFuture>>(m, "TcpRegionFuture")
        .def("wait", &RegionFuture::wait, py::call_guard<py::gil_scoped_release>())
        .def(
            "wait_for",
            [](RegionFuture& f, int64_t ms) -> py::object {
                int32_t status;
                bool    done;
                {
                    py::gil_scoped_release release;
                    done = f.wait_for(ms, &status);
                }
                return done ? py::cast(status) : py::none();
            },
            py::arg("timeout_ms"))
        .def("cancel", &RegionFuture::cancel, py::call_guard<py::gil_scoped_release>());
    py::class_<TcpRegionEndpoint>(m, "TcpRegionEndpoint")
        .def(py::init<const std::string&, uint16_t, RegionOptions>(),
             py::arg("ip"),
             py::arg("port")    = 0,
             py::arg("options") = RegionOptions{})
        .def("regions", &TcpRegionEndpoint::regions)
        .def("endpoint_info", &TcpRegionEndpoint::endpoint_info)
        .def("read",
             &TcpRegionEndpoint::read,
             py::arg("peer"),
             py::arg("spans"),
             py::arg("timeout_ms") = 30000,
             py::call_guard<py::gil_scoped_release>())
        .def("write",
             &TcpRegionEndpoint::write,
             py::arg("peer"),
             py::arg("spans"),
             py::arg("timeout_ms") = 30000,
             py::call_guard<py::gil_scoped_release>())
        .def("shutdown", &TcpRegionEndpoint::shutdown, py::call_guard<py::gil_scoped_release>());
}
