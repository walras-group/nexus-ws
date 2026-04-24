## Python Rules

1. using uv as the default package manager for Python projects
2. do not write pyproject.toml files, if not exits, using `uv init` or `uv init --lib` to create one
3. add packages to the project using `uv add <package-name>` command and add dev dependencies using `uv add <package-name> --dev` command
4. tests using pytest, and add it as a dev dependency using `uv add pytest --dev` command and uv run pytest to run the tests
5. use `uv run <command>` to run any command in the project, for example `uv run python main.py` to run the main.py file
6. using `ruff` as the linter for Python projects, and add it as a dev dependency using `uv add ruff --dev` command and uv run ruff to run the linter. 
7. **DO NOT** run `compileall` command to compile the Python files, as it is not necessary.