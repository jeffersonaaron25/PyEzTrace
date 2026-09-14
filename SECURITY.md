# Security policy

Security fixes target the latest published release. Older releases do not have a
separate backport commitment; update to the latest release when reporting a problem.

## Report a vulnerability privately

Email the maintainer at **jefferson.nelsson@gmail.com** with the subject
`PyEzTrace security report`. Include the affected version, impact, and a minimal
reproduction using synthetic data. Do not post exploit details or sensitive logs
in a public issue. The project does not promise a fixed response or fix deadline.

## Intended deployment and data handling

The trace viewer is for local development and analysis. Keep its default loopback
binding; it has no authentication or multi-tenant access control. Binding it to a
network interface exposes trace data to anyone who can reach that address.

Trace arguments, results, log context, and payload files can contain sensitive
information. Limit capture and retention, configure redaction for your data, and
review files before sharing them. Redaction is not a guarantee that arbitrary
objects or application messages contain no secrets.

OTEL and storage exporters can send traces outside the machine. Configure the
endpoint and credentials deliberately, and keep credentials out of shared logs.
Console export and console fallback may also expose trace data through stdout.

See the [configuration guide](https://jeffersonaaron25.github.io/PyEzTrace/configuration/)
for current settings.
