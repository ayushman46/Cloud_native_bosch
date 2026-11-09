# Who built what

Fill this in before submission. Panels ask about it, and the repository should show the answer.

## Team
- Name, role, what they built: _(to fill in)_

## AI assistance
Large parts of this repository were written with an AI coding assistant (Claude Code) under the team's direction: the Observer and Analyzer (`backend/chaosai/insight/`), the merge into one backend, the Chaos Control service, the resilience score, the automatic stop, the dashboard rebuild, and the documentation. Prompts, design decisions and review came from the team; every design choice is recorded in [DECISIONS.md](DECISIONS.md).

The original orchestrator and Planner were written by other team members before this consolidation and are in the earlier commits.

## What each member should be able to explain without opening the code
1. The experiment flow end to end and where each safety check sits.
2. Why the controller is separate and what it refuses to do.
3. How the resilience score is computed and what "unmeasured" means.
4. What the automatic stop watches and what happens after it fires.
5. What is simulated in mock mode and what is measured in real mode.
