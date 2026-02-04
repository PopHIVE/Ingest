# Commit, Push, and Create PR

Automate the git workflow for completing a feature or fix.

## Usage

```
/commit-push-pr [optional commit message]
```

## Pre-computed Context

Before proceeding, gather this information:
- Current branch: `git branch --show-current`
- Git status: `git status --short`
- Recent commits on this branch: `git log --oneline -5`
- Diff summary: `git diff --stat`

## Workflow

1. **Review Changes**
   - Check `git status` for all modified/added files
   - Review the diff to understand what's being committed
   - Ensure no sensitive files are staged (.env, credentials, etc.)

2. **Run Pre-commit Checks** (if applicable)
   - For R files: Check syntax with `Rscript -e "parse(file='<file>')"`
   - Verify data files are compressed (.csv.gz, .parquet)

3. **Stage and Commit**
   - Stage relevant files: `git add <files>`
   - Create a commit with Conventional Commits format:
     - `feat:` for new features (new data sources, new bundles)
     - `fix:` for bug fixes (data corrections, pipeline fixes)
     - `docs:` for documentation (CLAUDE.md, README updates)
     - `refactor:` for refactoring (code cleanup)
     - `data:` for data updates (new raw data, re-processed outputs)
     - `chore:` for maintenance (dependency updates)
   - Write a clear, concise commit message focusing on "why"
   - Include co-author line: `Co-Authored-By: Claude <noreply@anthropic.com>`

4. **Push to Remote**
   - Push the branch: `git push -u origin HEAD`
   - If branch doesn't exist on remote, create it

5. **Create Pull Request**
   - Use GitHub CLI: `gh pr create`
   - Include:
     - Clear title summarizing the change
     - Description with summary and context
     - Reference any related issues
   - Format PR body with:
     ```
     ## Summary
     <1-3 bullet points>

     ## Test plan
     [Bulleted checklist of testing steps]

     🤖 Generated with [Claude Code](https://claude.ai/code)
     ```

## Arguments

Pass a commit message or leave empty for auto-generated message based on changes.

Example: `/commit-push-pr feat: add mmr_epic data source`

## Output

Return the PR URL when complete.
