# Plan 15 — Packaging and Documentation

## Goal

Prepare the library for public PyPI release and publish complete documentation.

## Scope

### PyPI Packaging
- Final `pyproject.toml` review: classifiers, keywords, version, optional dependencies
- `CHANGELOG.md` with initial release notes
- Verify `python -m build` produces a valid wheel and sdist
- Publish to PyPI via `twine` or `hatch publish`

### Documentation (MkDocs)
- `docs/index.md` — project overview and quickstart
- `docs/concepts.md` — conceptual guide (universe/variables contract, check families, resolver dispatch)
- `docs/api/` — auto-generated API reference via `mkdocstrings`
- `docs/examples/` — prose walkthrough of the notebooks from Plan 14
- `mkdocs.yml` configuration

### CI Additions
- Add `publish` workflow to `.github/workflows/` triggered on version tags
- Add documentation build check to CI

### Final Checklist Before Release
- [ ] All 30+ tests pass on Python 3.10, 3.11, 3.12
- [ ] `ruff check` passes with zero warnings
- [ ] `mypy` passes in strict mode
- [ ] Package installs cleanly from PyPI test index
- [ ] README accurately reflects public API
- [ ] `CHANGELOG.md` written
- [ ] Version bumped to `0.1.0` (or appropriate)
- [ ] GitHub release created with tag
- [ ] Committed and pushed to `main`
