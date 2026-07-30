---
version: 1
robot_id: piper
policy_id: sam3
policy_command_topic: gateway/policy_command
status_topic: sam3_policy/policy_command_status
actions:
  grasp:
    command: grasp_simple
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
      target_name: target_name
    resources: ["arm", "gripper", "camera"]
    timeout_s: 120
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Grasp a target object with SAM3 perception."

  place:
    command: place_simple
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
      target_name: target_name
    resources: ["arm", "gripper", "camera"]
    timeout_s: 120
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Place the held object at a target area."

  check_target:
    command: check_target
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
      target_name: target_name
    resources: ["camera"]
    timeout_s: 30
    result_semantics: task_verified
    completion:
      type: policy_status
    description: "Check whether a target object is visible."

  go_home:
    command: go_standby
    required_parameters: []
    input_mapping: {}
    resources: ["arm", "gripper"]
    timeout_s: 30
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Move the Piper arm to the standby pose."
---

# SAM3 Piper Actions

This manifest describes the MVP+ PAOS-facing actions exposed by the SAM3 policy on the Piper robot.

## Notes

- Gateway parses only the YAML frontmatter above.
- Actions are executed serially by Gateway in MVP+.
- `succeeded` means the policy returned command-level success; physical task verification may still require additional observation.
