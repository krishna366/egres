# Local Agent Notes

- In this environment, run `noprompt` in the shell session before running commands to avoid starship/fnm permission-noise.
- Prefer commands through an already initialized shell session where `noprompt` has been executed.
- Code style: keep lines at 100 characters or fewer unless a longer line is unavoidable.
- When adding or changing non-trivial logic, add comments that explain intent and invariants, not
  just mechanics.
- Commenting practice:
  - Keep comments concise and close to the relevant block.
  - Explain why the code exists, assumptions, and edge cases when not obvious.
  - Avoid redundant comments that restate the code line-by-line.
