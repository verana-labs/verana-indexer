import {
  anyTypeUrl,
  decideProposalOutcome,
  durationToSeconds,
  GROUP_MSG_TYPES,
  GROUP_ROUTED_MESSAGE_TYPES,
  isUndecodedAny,
  normalizeTallyResult,
  parseEventAttrJson,
  parseEventAttrValue,
  readPositiveInt,
  shortExecutorResult,
  shortProposalStatus,
  shortVoteOption,
  tallyFromVotes,
  votingPeriodSecondsFromDecisionPolicy,
} from '../../../../src/services/crawl-group/group_helpers'

describe('group_helpers.GROUP_ROUTED_MESSAGE_TYPES', () => {
  it('routes every x/group message and excludes the verana create message', () => {
    expect(GROUP_ROUTED_MESSAGE_TYPES).toHaveLength(Object.keys(GROUP_MSG_TYPES).length - 1)
    expect(GROUP_ROUTED_MESSAGE_TYPES).not.toContain(GROUP_MSG_TYPES.CreateCorporation)
    expect(GROUP_ROUTED_MESSAGE_TYPES).toContain(GROUP_MSG_TYPES.LeaveGroup)
    expect(GROUP_ROUTED_MESSAGE_TYPES.every((type) => type.startsWith('/cosmos.group.v1.'))).toBe(true)
  })
})

describe('group_helpers enum mapping', () => {
  it('maps proposal statuses from names and numbers to spec short forms', () => {
    expect(shortProposalStatus('PROPOSAL_STATUS_SUBMITTED')).toBe('SUBMITTED')
    expect(shortProposalStatus('PROPOSAL_STATUS_WITHDRAWN')).toBe('WITHDRAWN')
    expect(shortProposalStatus('ACCEPTED')).toBe('ACCEPTED')
    expect(shortProposalStatus(3)).toBe('REJECTED')
    expect(shortProposalStatus(4)).toBe('ABORTED')
    expect(shortProposalStatus('PROPOSAL_STATUS_UNSPECIFIED')).toBeNull()
    expect(shortProposalStatus(undefined)).toBeNull()
  })

  it('maps executor results', () => {
    expect(shortExecutorResult('PROPOSAL_EXECUTOR_RESULT_NOT_RUN')).toBe('NOT_RUN')
    expect(shortExecutorResult('PROPOSAL_EXECUTOR_RESULT_SUCCESS')).toBe('SUCCESS')
    expect(shortExecutorResult(3)).toBe('FAILURE')
    expect(shortExecutorResult('bogus')).toBeNull()
  })

  it('maps vote options including the 2=ABSTAIN/3=NO wire order', () => {
    expect(shortVoteOption('VOTE_OPTION_YES')).toBe('YES')
    expect(shortVoteOption('VOTE_OPTION_NO_WITH_VETO')).toBe('NO_WITH_VETO')
    expect(shortVoteOption(2)).toBe('ABSTAIN')
    expect(shortVoteOption(3)).toBe('NO')
    expect(shortVoteOption(0)).toBeNull()
  })
})

describe('group_helpers event attribute parsing', () => {
  it('strips the JSON quoting of typed-event attribute values', () => {
    expect(parseEventAttrValue('"5"')).toBe('5')
    expect(parseEventAttrValue('"verana1abc"')).toBe('verana1abc')
    expect(parseEventAttrValue('5')).toBe('5')
    expect(parseEventAttrValue(undefined)).toBeNull()
  })

  it('parses embedded JSON objects (EventProposalPruned tally_result)', () => {
    expect(parseEventAttrJson('{"yes_count":"2","no_count":"0"}')).toEqual({ yes_count: '2', no_count: '0' })
    expect(parseEventAttrJson('not-json')).toBeNull()
    expect(parseEventAttrJson(undefined)).toBeNull()
  })

  it('reads positive integers from strings', () => {
    expect(readPositiveInt('7')).toBe(7)
    expect(readPositiveInt('0')).toBeNull()
    expect(readPositiveInt('abc')).toBeNull()
  })
})

describe('group_helpers durations and tallies', () => {
  it('parses cosmos duration JSON', () => {
    expect(durationToSeconds('300s')).toBe(300)
    expect(durationToSeconds('0.5s')).toBe(0.5)
    expect(durationToSeconds('300')).toBeNull()
    expect(durationToSeconds(300)).toBeNull()
  })

  it('extracts the voting period from a verbatim decision policy', () => {
    const policy = {
      '@type': '/cosmos.group.v1.ThresholdDecisionPolicy',
      threshold: '2',
      windows: { voting_period: '300s', min_execution_period: '0s' },
    }
    expect(votingPeriodSecondsFromDecisionPolicy(policy)).toBe(300)
    expect(votingPeriodSecondsFromDecisionPolicy(null)).toBeNull()
    expect(votingPeriodSecondsFromDecisionPolicy({})).toBeNull()
  })

  it('normalizes x/group TallyResult field names', () => {
    expect(
      normalizeTallyResult({ yes_count: '2', no_count: '1', abstain_count: '0', no_with_veto_count: '0' })
    ).toEqual({ yes_count: '2', no_count: '1', abstain_count: '0', no_with_veto_count: '0' })
    expect(normalizeTallyResult({ yesCount: '3' })).toEqual({
      yes_count: '3',
      no_count: '0',
      abstain_count: '0',
      no_with_veto_count: '0',
    })
    expect(normalizeTallyResult(null)).toBeNull()
  })

  it('counts votes per option', () => {
    expect(tallyFromVotes([{ option: 'YES' }, { option: 'YES' }, { option: 'NO' }, { option: 'ABSTAIN' }])).toEqual({
      yes_count: '2',
      no_count: '1',
      abstain_count: '1',
      no_with_veto_count: '0',
    })
  })
})

describe('group_helpers.decideProposalOutcome', () => {
  const threshold = { '@type': '/cosmos.group.v1.ThresholdDecisionPolicy', threshold: '2' }
  const percentage = { '@type': '/cosmos.group.v1.PercentageDecisionPolicy', percentage: '0.5' }

  it('accepts a threshold policy once yes weight reaches the threshold', () => {
    expect(decideProposalOutcome(threshold, 2, 3)).toBe('ACCEPTED')
    expect(decideProposalOutcome(threshold, 3, 3)).toBe('ACCEPTED')
  })

  it('rejects a threshold policy below the threshold', () => {
    expect(decideProposalOutcome(threshold, 1, 3)).toBe('REJECTED')
    expect(decideProposalOutcome(threshold, 0, 3)).toBe('REJECTED')
  })

  it('evaluates a percentage policy against total group weight', () => {
    expect(decideProposalOutcome(percentage, 2, 4)).toBe('ACCEPTED')
    expect(decideProposalOutcome(percentage, 3, 4)).toBe('ACCEPTED')
    expect(decideProposalOutcome(percentage, 1, 4)).toBe('REJECTED')
  })

  it('caps the threshold at total weight, so a shrunken group can still accept', () => {
    // members left until total weight (2) fell below the threshold (3)
    expect(decideProposalOutcome({ '@type': '/cosmos.group.v1.ThresholdDecisionPolicy', threshold: '3' }, 2, 2)).toBe(
      'ACCEPTED'
    )
    expect(decideProposalOutcome({ '@type': '/cosmos.group.v1.ThresholdDecisionPolicy', threshold: '3' }, 1, 2)).toBe(
      'REJECTED'
    )
  })

  it('returns null rather than guessing when the policy cannot be evaluated', () => {
    expect(decideProposalOutcome(null, 5, 5)).toBeNull()
    expect(decideProposalOutcome({}, 5, 5)).toBeNull()
    expect(decideProposalOutcome({ '@type': '/cosmos.group.v1.SomeFuturePolicy' }, 5, 5)).toBeNull()
    expect(decideProposalOutcome({ ...threshold, threshold: 'abc' }, 5, 5)).toBeNull()
    // percentage is undecidable without a positive total weight
    expect(decideProposalOutcome(percentage, 1, 0)).toBeNull()
  })

  it('reads the policy shape even when @type is absent', () => {
    expect(decideProposalOutcome({ threshold: '2' }, 2, 3)).toBe('ACCEPTED')
    expect(decideProposalOutcome({ percentage: '0.5' }, 1, 4)).toBe('REJECTED')
  })
})

describe('group_helpers Any handling', () => {
  it('reads the type url in any of its encodings', () => {
    expect(anyTypeUrl({ '@type': '/a.b.MsgX' })).toBe('/a.b.MsgX')
    expect(anyTypeUrl({ type_url: '/a.b.MsgX' })).toBe('/a.b.MsgX')
    expect(anyTypeUrl({ typeUrl: '/a.b.MsgX' })).toBe('/a.b.MsgX')
    expect(anyTypeUrl({})).toBeNull()
  })

  it('detects undecoded base64 payloads', () => {
    expect(isUndecodedAny({ type_url: '/a.b.MsgX', value: 'CgFh' })).toBe(true)
    expect(isUndecodedAny({ '@type': '/a.b.MsgX', voter: 'verana1x', value: 'CgFh' })).toBe(false)
    expect(isUndecodedAny({ '@type': '/a.b.MsgX', voter: 'verana1x' })).toBe(false)
  })
})
