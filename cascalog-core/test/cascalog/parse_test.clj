(ns cascalog.parse-test
  (:use midje.sweet
        cascalog.parse))

(fact
  "parse-variables expands selectors out into the proper unsugared
   forms."
  (parse-variables ['?a '?b :> 4] :>) => {:<< ['?a '?b] :>> [4]}

  "If a selector exists in the sequence, the default is ignored."
  (parse-variables ['?a '?b :> 4] :<) => {:<< ['?a '?b] :>> [4]}

  "In the absence, the default is used. See input:"
  (parse-variables ['?a '?b] :<) => {:<< ['?a '?b]}

  "or output."
  (parse-variables ['?a '?b] :>) => {:>> ['?a '?b]}

  "Only :< and :> are allowed as the default selector."
  (parse-variables ['?a '?b :> 4] :anything) => (throws AssertionError)

  )
