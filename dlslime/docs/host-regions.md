# Owned HOST regions

`HostRegionRegistry` is independent of TCP and verbs. It accepts fixed HOST
allocations (`HostBuffer`) or an explicitly owned HOST range. An external owner
must retain the actual allocation; an unrelated/no-op owner, independently freed
memory, resizable storage, and device pointers violate this API's preconditions.
No pointer classification or device runtime is implied by this foundation.

Each export identifies the registry incarnation (128 random bits), slot, and
generation, plus length and permissions. No virtual address is exported. Slots
can be reused only after all leases have retired and the registry's allocation
owner has been released; generation never wraps. Overlapping live/retiring
ranges in a registry are rejected. Different registries must not be used to
circumvent an allocation's lifetime or application synchronization.

`acquire` atomically checks scope/generation, admission, permissions and range.
A move-only lease retains the allocation. `retire` closes admission immediately
and returns a shared future; it never blocks the I/O thread. That future becomes
ready only after all leases and the registry's owner reference retire. It does
not revoke references retained by the application. Repeated retire is supported
while draining; a key already removed is stale. Closing the registry stops all
new registrations/admissions; existing leases remain safe even after destruction.
Owner deleters run outside the registry lock. A deleter must not synchronously
wait for its own retirement or throw.

Permissions are access capabilities, not a data-race lock. Applications must
establish producer readiness and avoid concurrent conflicting access. This PR
does not register hardware MRs, implement device visibility, authenticate peers,
or change the legacy raw-address TCP API. The acknowledged TCP protocol builds
on these leases; device allocation and multi-PD ownership remain separate work.

Standalone tests: configure `tests/cpp/region` with CMake and optionally
`-DREGION_SANITIZE=ON`, build, and run CTest. Run only in the designated development
containers for this project.
