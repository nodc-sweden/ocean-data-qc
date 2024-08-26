# ocean_data_qc

Automatic quality control of ocean data.

## Setup

### Python and PDM

This library uses the dependency manager PDM. To setup a working development environment,
you need the targeted python version (currently Python 3.11).

PDM itself is installed with pip. To make sure that PDM is always available, it can be
advisable to use pipx instead of pip. To get started with this, follow instructions at
https://pipx.pypa.io/stable/.

### Virtual environment

Next, you can choose to handle virtual environments yourself or let PDM create it.

#### Using PDM

Select the correct Python installation with the following command:

```bash
$ pdm use
```

The create the environment with:

```bash
$ pdm venv create
```

#### Manually handle virtual environment

With your own virtual environment (e.g. using `python -m venv venv`) you need to make sure
that PDM knows about it. The easiest way is to manually add the full path to the python
executable of the environment of to a file called `.pdm-python` in the project root.

Windows:

```txt
C:\JohnP\code\fyskemqc\venv\Scripts\python.exe
```

Mac/Linux:

```txt
/home/JohnP/code/fyskemqc/venv/bin/python
```

### pre-commit

To manage git pre-commit hooks, the tool `pre-commit` is used. This tool is installed as a
dev-dependency but it must be activated with the following command:

```bash
$ pre-commit install
```

After activation, a commit will be aborted if a series of checks are not passed. Which
checks should run is configured in `.pre-commit-config.yaml`.

Do avoid the checks (e.g. when commiting to a topic branch), the`--no-verify` flag can be
added to the commit command.

```bash
$ git commit --no-verify
```

## Working with the project

### Handling dependencies

PDM will handle all dependencies, so you should never install anything manually with pip.
Add new dependencies with the following command:

```bash
$ pdm add pandas
```

Dependencies that only are used for development (e.g. testing, formatting etc.) areinstalled
with the -d flag:

```bash
$ pdm add -d pytest
```

To install all configured dependencies (including development dependencies) into the virtual
environment, use the following command:

```bash
$ pdm install -d
```

You can also use use the sync command which is advisable after pulling remote code where
someone else might have added or updated the dependencies.

```bash
$ pdm sync
```

### ruff

The code is linted and formated by `ruff`.

#### Lintning

Lint the code with the following command:

```bash
$ ruff check
```

This will check that the code complies to the configured linting rules. The configuration
is stored in `pyproject.yaml` under the section `[tool.ruff.lint]`. Some identified problems
can be fixed by `ruff` by adding the `--fix` flag.

```bash
$ ruff check --fix
```

Information about all available rules can be found here:

- https://docs.astral.sh/ruff/rules/

#### Formatering

Format the code with the following command:

```bash
$ ruff format
```

### Testing

You can run all tests using PDM with the following command in the project root:

```bash
$ pdm run pytest
```

This is equivalent to running pytest inside the configured virtualenv.

```bash
(venv) $ pytest
```

## End user installations

The easiest way to install `fyskemqc`is by pointing pip to github. This will install the
latest version:

```bash
$ pip install git+https://github.com/nodc-sweden/fyskemqc  
```

To install a specific version, add the following version specifier to end of the url:

```bash
$ pip install git+https://github.com/nodc-sweden/fyskemqc@v0.1.0
```

Available versions can be found here:

- https://github.com/nodc-sweden/fyskemqc/releases

To quickly share the local state with someone without using git, you can build a wheel
using the command:

```bash
$ pdm build
```

The resulting wheel is saved in the`dist` directory. Installing it with pip will make sure
that all dependencies are also installed.
