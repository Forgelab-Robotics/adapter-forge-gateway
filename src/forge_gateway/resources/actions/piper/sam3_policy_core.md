---
version: 1
robot_id: piper
policy_id: sam3
policy_command_topic: gateway/policy_command
status_topic: policy_core/policy_command_status
actions:
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
    description: "Check whether a target object is visible through split policy_core + sam3_detection."

  check_target_shelf:
    command: check_target_shelf
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
      target_name: target_name
    resources: ["camera"]
    timeout_s: 30
    result_semantics: task_verified
    completion:
      type: policy_status
    description: "Check whether a shelf target is visible from the current wrist-camera pose without moving to standby."

  check_target_shelf_sam_video:
    command: check_target_shelf_sam_video
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
      target_name: target_name
    resources: ["camera"]
    timeout_s: 45
    result_semantics: task_verified
    completion:
      type: policy_status
    description: "Check whether a shelf target is visible using video-style multi-frame SAM perception."

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
    description: "Grasp a target object through policy_core, SAM3 detection, heuristic planning, and motion capability."

  grasp_shelf:
    command: grasp_shelf
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
      target_name: target_name
    resources: ["arm", "gripper", "camera"]
    timeout_s: 120
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Grasp a shelf target from the current wrist-camera pose without first moving to the desktop standby pose."

  grasp_shelf_sam_video:
    command: grasp_shelf_sam_video
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
      target_name: target_name
    resources: ["arm", "gripper", "camera"]
    timeout_s: 150
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Grasp a shelf target using video-style multi-frame SAM perception, heuristic planning, and motion capability."

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
    description: "Place the held object through split policy_core workflow."

  place_table_front:
    command: place_table_front
    required_parameters: []
    input_mapping: {}
    resources: ["arm", "gripper"]
    timeout_s: 60
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Place the held object at a fixed open-table pose in front of the Piper base without perception."

  go_home:
    command: go_standby
    required_parameters: []
    input_mapping: {}
    resources: ["arm", "gripper"]
    timeout_s: 30
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Move the Piper arm to the standby pose through pybullet_ik_motion."

  gripper_open:
    command: gripper_open
    required_parameters: []
    input_mapping: {}
    resources: ["gripper"]
    timeout_s: 10
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Open the Piper gripper through pybullet_ik_motion."

  gripper_close:
    command: gripper_close
    required_parameters: []
    input_mapping: {}
    resources: ["gripper"]
    timeout_s: 10
    result_semantics: command_completed
    completion:
      type: policy_status
    description: "Close the Piper gripper through pybullet_ik_motion."
---

# SAM3 Piper Split-Policy Actions

This manifest exposes the PAOS Gateway action surface for split-policy:

```text
Gateway -> policy_core -> sam3_detection / sam_video_detection / heuristic_manipulation_planner / pybullet_ik_motion
```

Gateway parses only the YAML frontmatter. The body is for Agent/human context.
