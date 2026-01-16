# AI Agent Instructions: Data Fabric Domain

## Domain Overview

This is the **Data Fabric** domain plugin for AIVerse. It provides stateless execution units
for data operations including ingestion, transformation, feature engineering, versioning,
labeling, and quality validation.

## Key Constraints

### Stateless Execution
- All execution units MUST be stateless
- No internal caching, sessions, or accumulated state
- Units can be terminated at any point with no side effects

### No Orchestration
- Execution units do NOT call other execution units
- No loops, retries, or conditionals within units
- MCOP handles all scheduling and sequencing

### Domain Independence
- This domain can be fully removed without breaking the Control Plane
- No cross-domain imports or dependencies
- All interactions via published Control Plane interfaces

## File Organization

- `execution_units/` - Python implementations of 22 execution units
- `registry_cards/` - JSON metadata for registry registration
- `feedback_signals/` - Metrics, outcomes, and advisor signal definitions
- `schemas/` - Input/output contract dataclasses

## Testing Approach

- Unit tests should mock all Protocol dependencies
- Tests should verify failure modes produce correct error codes
- Tests should NOT test orchestration (there is none)
- Integration tests should verify registry card registration

## Common Mistakes to Avoid

1. Adding state to execution units
2. Calling one execution unit from another
3. Importing from other domains (model, agent, etc.)
4. Adding retry logic or loops
5. Assuming execution order
6. Referencing cluster identifiers or hardware specs

## ADR Reference

Always consult `docs/adr/PHASE3-DATA-FABRIC-PLUGIN-SPEC.md` for authoritative definitions.
