# Contributing to PyEzTrace

We love your input! We want to make contributing to PyEzTrace as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## Development Process

Pull requests are welcome!

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

To exercise the OpenTelemetry bridge tests locally, install the optional dependencies with:

```bash
pip install "pyeztrace[otel]"
```

## Any contributions you make will be under the MIT Software License

In short, when you submit code changes, your submissions are understood to be under the same [MIT License](http://choosealicense.com/licenses/mit/) that covers the project. Feel free to contact the maintainers if that's a concern.

## Report bugs using GitHub's issue tracker

Use the [issue chooser](https://github.com/jeffersonaaron25/PyEzTrace/issues/new/choose) to report a bug or propose a feature. Bug reports ask for the package and Python versions, operating system, setup options, a minimal reproduction, and expected versus actual behavior. Feature requests ask for the problem, proposed behavior, and current alternatives.

Use synthetic examples and remove credentials and personal data before attaching code, configuration, or logs.

## Write bug reports with detail, background, and sample code

**Great Bug Reports** tend to have:

- A quick summary and/or background
- Steps to reproduce
  - Be specific!
  - Give sample code if you can.
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

## License

By contributing, you agree that your contributions will be licensed under its MIT License.

## Local quality checks

Install `pytest`, `pytest-asyncio`, `coverage`, and `ruff` with the package's `all`
extra. Run `ruff check pyeztrace tests` and
`coverage run --branch --source=pyeztrace -m pytest`, then `coverage report --fail-under=60`.
The existing coverage floor remains 60%; CI also publishes a readable summary.
Ruff initially checks syntax and control-flow correctness, not all formatting rules.
For vulnerabilities, follow [SECURITY.md](SECURITY.md).
