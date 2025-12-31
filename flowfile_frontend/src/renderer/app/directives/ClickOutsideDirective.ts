import { DirectiveBinding, ObjectDirective } from 'vue'

const ClickOutsideDirective: ObjectDirective = {
  beforeMount(el: HTMLElement, binding: DirectiveBinding) {
    // Type assertion to bypass TypeScript's type checking
    const element = el as any

    element.clickOutsideEvent = (event: Event) => {
      if (!(element === event.target || element.contains(event.target as Node))) {
        binding.value(event)
      }
    }

    document.addEventListener('click', element.clickOutsideEvent)
  },
  unmounted(el: HTMLElement) {
    const element = el as any
    if (element.clickOutsideEvent) {
      document.removeEventListener('click', element.clickOutsideEvent)
    }
  },
}

export default ClickOutsideDirective
