A test to try out integrating Raft into a project.

It works and recovers correctly from failures. However, this is 
entirely a toy and not made to be durable or tested in any way.

I will likely work on extracting pieces from this and creating
replacements to better understand individual pieces of the system,
such as transport and storage.

Initially this was going to use etcd-io/raft, but the documentation is
fairly old and seems to be out of date? It also doesn't detail very much
about how it might be integrated. So I moved to the hashicorp raft implementation.

So far my impressions of the code quality and API for the hashicorp version are
significantly higher. It also comes batteries included, with a storage, 
snapshot system, and network transport.