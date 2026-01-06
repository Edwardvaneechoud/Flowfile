export interface SpellCheckError {
  type: "unclosed_quote" | "invalid_function" | "unmatched_parenthesis";
  index: number;
  character?: string;
}

interface StackItem {
  type: "'" | '"' | "(";
  index: number;
}

const regexPatterns = {
  addition: /\/\/[^//\r\n]*/,
  operator: /[+\-/%*&><]/,
  field: /\[[^\]]+\]/,
  string: /(["'])(?:\\(?:\r\n|[\s\S])|(?!\1)[^\\\r\n])*\1/,
};

const checkUnclosedQuotes = (code: string): SpellCheckError[] => {
  const errors: SpellCheckError[] = [];
  const stack: StackItem[] = [];

  for (let i = 0; i < code.length; i++) {
    const char = code[i];

    // Check if character is part of any group; if yes, skip
    let isSkipped = false;
    for (const pattern of Object.values(regexPatterns)) {
      const match = code.slice(i).match(pattern);
      if (match && match.index === 0) {
        i += match[0].length - 1; // Skip the length of the match - 1
        isSkipped = true;
        break;
      }
    }

    if (isSkipped) continue;

    if (char === "'" || char === '"') {
      if (stack.length > 0 && stack[stack.length - 1].type === char) {
        stack.pop();
      } else {
        stack.push({ type: char, index: i });
      }
    }
  }

  stack.forEach((unmatched) => {
    errors.push({ type: "unclosed_quote", index: unmatched.index, character: unmatched.type });
  });

  return errors;
};

const checkInvalidFunctions = (code: string, functionNames: string[]): SpellCheckError[] => {
  const errors: SpellCheckError[] = [];
  const functionPattern = new RegExp(`\\b(${functionNames.join("|")})\\b`, "i");
  let wordBuffer = "";

  for (let i = 0; i < code.length; i++) {
    const char = code[i];

    // Check if character is part of any group; if yes, skip
    let isSkipped = false;
    for (const pattern of Object.values(regexPatterns)) {
      const match = code.slice(i).match(pattern);
      if (match && match.index === 0) {
        i += match[0].length - 1; // Skip the length of the match - 1
        isSkipped = true;
        break;
      }
    }

    const integerMatch = code.slice(i).match(/\b\d+\b/);
    if (integerMatch && integerMatch.index === 0) {
      i += integerMatch[0].length - 1;
      isSkipped = true;
    }

    if (isSkipped) {
      wordBuffer = ""; // Clear the buffer since we're skipping
      continue;
    }

    if (char.match(/\w/)) {
      wordBuffer += char;
    } else {
      if (wordBuffer && !wordBuffer.match(functionPattern)) {
        errors.push({ type: "invalid_function", index: i - wordBuffer.length });
      }
      wordBuffer = "";
    }
  }

  if (wordBuffer && !wordBuffer.match(functionPattern)) {
    errors.push({ type: "invalid_function", index: code.length - wordBuffer.length });
  }

  return errors;
};

const checkMissingClosingParentheses = (code: string): SpellCheckError[] => {
  const errors: SpellCheckError[] = [];
  const stack: StackItem[] = [];

  for (let i = 0; i < code.length; i++) {
    const char = code[i];
    if (char === "(") {
      stack.push({ type: "(", index: i });
    } else if (char === ")") {
      if (stack.length === 0 || stack.pop()?.type !== "(") {
        errors.push({ type: "unmatched_parenthesis", index: i });
      }
    }
  }

  // Remaining unmatched parentheses
  stack.forEach((unmatched) => {
    errors.push({ type: "unmatched_parenthesis", index: unmatched.index });
  });

  return errors;
};

export const spellCheck = (code: string, functionNames: string[]): SpellCheckError[] => {
  const unclosedQuoteErrors = checkUnclosedQuotes(code);
  const invalidFunctionErrors = checkInvalidFunctions(code, functionNames);
  const missingClosingParenthesesErrors = checkMissingClosingParentheses(code);
  return [...unclosedQuoteErrors, ...invalidFunctionErrors, ...missingClosingParenthesesErrors];
};
